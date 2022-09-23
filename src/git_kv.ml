
module Store = Git.Mem.Make(Digestif.SHA1)
module Sync = Git.Mem.Sync(Store)
module Search = Git.Search.Make(Digestif.SHA1)(Store)
module Git_commit = Git.Commit.Make(Store.Hash)

type t = {
  ctx : Mimic.ctx ;
  edn : Smart_git.Endpoint.t;
  branch : Git.Reference.t ;
  store : Store.t ;
  mutable head : Store.hash option ;
}

let init_store () =
  let open Lwt.Infix in
  Store.v (Fpath.v ".") >|= fun r ->
  Result.map_error
    (fun e -> `Msg (Fmt.str "error setting up store %a" Store.pp_error e))
    r

let main = Git.Reference.v "refs/heads/main"

let capabilities =
  [ `Side_band_64k; `Multi_ack_detailed; `Ofs_delta; `Thin_pack; `Report_status ]

let to_invalid = function
  | Ok x -> x
  | Error `Msg m -> invalid_arg m

let split_url s =
  match String.split_on_char '#' s with
  | [ edn; branch ] ->
    Smart_git.Endpoint.of_string edn |> to_invalid,
    Git.Reference.of_string ("refs/heads/" ^ branch) |> to_invalid
  | _ ->
    Smart_git.Endpoint.of_string s |> to_invalid, main

let fpath_to_key ~root v =
  Mirage_kv.Key.v (Fpath.to_string (Option.get (Fpath.relativize ~root v)))

let diff store commit0 commit1 =
  let open Lwt.Infix in
  let root = Fpath.v "." in
  let tbl0 = Hashtbl.create 0x10 in
  Store.fold store (fun () ?name ~length:_ hash _value ->
    Option.iter (fun name -> Hashtbl.add tbl0 (fpath_to_key ~root name) hash) name ;
    Lwt.return ()) () ~path:root commit0 >>= fun () ->
  let tbl1 = Hashtbl.create 0x10 in
  Store.fold store (fun () ?name ~length:_ hash _value ->
    Option.iter (fun name -> Hashtbl.add tbl1 (fpath_to_key ~root name) hash) name ;
    Lwt.return ()) () ~path:root commit1 >>= fun () ->
  let diff = Hashtbl.fold (fun name hash diff -> match Hashtbl.find_opt tbl1 name with
    | Some hash' when not (Digestif.SHA1.equal hash hash') -> `Change name :: diff
    | Some _ -> diff
    | None -> `Remove name :: diff) tbl0 [] in
  let diff = Hashtbl.fold (fun name _hash diff ->
    if Hashtbl.mem tbl0 name
    then `Add name :: diff else diff) tbl1 diff in
  Lwt.return diff

let diff store commit0 commit1 = match commit0 with
  | Some commit0 -> diff store commit0 commit1
  | None ->
    let root = Fpath.v "." in
    Store.fold store (fun diff ?name ~length:_ _hash _value -> match name with
      | None -> Lwt.return diff
      | Some name -> Lwt.return (`Add (fpath_to_key ~root name) :: diff)) [] ~path:root commit1

let pull t =
  let open Lwt.Infix in
  Sync.fetch ~capabilities ~ctx:t.ctx t.edn t.store ~deepen:(`Depth 1) (`Some [ t.branch, t.branch ]) >>= fun r ->
  let data =
    Result.map_error
      (fun e -> `Msg (Fmt.str "error fetching: %a" Sync.pp_error e))
      r
  in
  match data with
  | Error _ as e -> Lwt.return e
  | Ok None -> Lwt.return (Ok [])
  | Ok Some (_, _) ->
    Store.Ref.resolve t.store t.branch >>= fun r ->
    let head =
      Result.map_error
        (fun e -> `Msg (Fmt.str "error resolving branch %s: %a"
                          (Git.Reference.to_string t.branch)
                          Store.pp_error e))
        r |> to_invalid
    in
    diff t.store t.head head >>= fun diff ->
    t.head <- Some head;
    Lwt.return (Ok diff)

let connect ctx endpoint =
  let open Lwt.Infix in
  init_store () >>= fun store ->
  let store = to_invalid store in
  let edn, branch = split_url endpoint in
  let t = { ctx ; edn ; branch ; store ; head = None } in
  pull t >>= fun r ->
  let _r = to_invalid r in
  Lwt.return t

type key = Mirage_kv.Key.t

type change = [
  | `Add of key
  | `Remove of key
  | `Change of key
]

type error = Mirage_kv.error

let pp_error ppf = Mirage_kv.pp_error ppf

let disconnect _t = Lwt.return_unit

module SHA1 = struct
  include Digestif.SHA1

  let hash x = Hashtbl.hash x
end

module Verbose = struct
  type 'a fiber = 'a Lwt.t

  let succ _ = Lwt.return_unit
  let print _ = Lwt.return_unit
end

module Delta = Carton_lwt.Enc.Delta (SHA1) (Verbose)

let pack t ~commit stream =
  let open Lwt.Infix in
  let load t hash =
    Store.read_inflated t hash >|= function
    | None -> Fmt.failwith "%a not found" Digestif.SHA1.pp hash
    | Some (`Commit, cs) -> Carton.Dec.v ~kind:`A (Cstruct.to_bigarray cs)
    | Some (`Tree,   cs) -> Carton.Dec.v ~kind:`B (Cstruct.to_bigarray cs)
    | Some (`Blob,   cs) -> Carton.Dec.v ~kind:`C (Cstruct.to_bigarray cs)
    | Some (`Tag,    cs) -> Carton.Dec.v ~kind:`D (Cstruct.to_bigarray cs) in
  let to_entry ~length hash = function
    | Git.Value.Commit _ -> Carton_lwt.Enc.make_entry ~kind:`A ~length hash
    | Git.Value.Tree _   -> Carton_lwt.Enc.make_entry ~kind:`B ~length hash
    | Git.Value.Blob _   -> Carton_lwt.Enc.make_entry ~kind:`C ~length hash
    | Git.Value.Tag _    -> Carton_lwt.Enc.make_entry ~kind:`D ~length hash in
  Store.fold t (fun acc ?name:_ ~length hash value ->
    Lwt.return ((to_entry ~length:(Int64.to_int length) hash value) :: acc))
    ~path:(Fpath.v ".") [] commit >|= Array.of_list >>= fun entries ->
  Delta.delta ~threads:(List.init 4 (fun _ -> load t))
    ~weight:10 ~uid_ln:Digestif.SHA1.digest_size entries
  >>= fun targets ->
  let offsets = Hashtbl.create (Array.length targets) in
  let find hash = Lwt.return (Option.map Int64.to_int (Hashtbl.find_opt offsets hash)) in
  let uid =
    { Carton.Enc.uid_ln= SHA1.digest_size
    ; Carton.Enc.uid_rw= SHA1.to_raw_string } in
  let b =
    { Carton.Enc.o= Bigstringaf.create De.io_buffer_size
    ; Carton.Enc.i= Bigstringaf.create De.io_buffer_size
    ; Carton.Enc.q= De.Queue.create 0x10000
    ; Carton.Enc.w= De.Lz77.make_window ~bits:15 } in
  let ctx = ref SHA1.empty in
  let cursor = ref 0L in
  let header = Bigstringaf.create 12 in
  Carton.Enc.header_of_pack ~length:(Array.length targets) header 0 12 ;
  stream (Some (Bigstringaf.to_string header)) ;
  ctx := SHA1.feed_bigstring !ctx header ~off:0 ~len:12 ;
  cursor := Int64.add !cursor 12L ;
  let encode_target idx =
    Hashtbl.add offsets (Carton.Enc.target_uid targets.(idx)) !cursor ;
    Carton_lwt.Enc.encode_target ~b ~find ~load:(load t) ~uid targets.(idx) ~cursor:(Int64.to_int !cursor)
    >>= fun (len, encoder) ->
    let payload = Bigstringaf.substring b.o ~off:0 ~len in
    stream (Some payload) ;
    ctx := SHA1.feed_bigstring !ctx b.o ~off:0 ~len ;
    cursor := Int64.add !cursor (Int64.of_int len) ;
    let rec go encoder = match Carton.Enc.N.encode ~o:b.o encoder with
      | `Flush (encoder, len) ->
        let payload = Bigstringaf.substring b.o ~off:0 ~len in
        stream (Some payload) ;
        ctx := SHA1.feed_bigstring !ctx b.o ~off:0 ~len ;
        cursor := Int64.add !cursor (Int64.of_int len) ;
        let encoder = Carton.Enc.N.dst encoder b.o 0 (Bigstringaf.length b.o) in
        go encoder
      | `End -> Lwt.return_unit in
    let encoder = Carton.Enc.N.dst encoder b.o 0 (Bigstringaf.length b.o) in
    go encoder in
  let rec go idx =
    if idx < Array.length targets
    then encode_target idx >>= fun () -> go (succ idx)
    else Lwt.return_unit in
  go 0 >>= fun () ->
  let hash = SHA1.get !ctx in
  stream (Some (SHA1.to_raw_string hash)) ;
  stream (Some (SHA1.to_raw_string commit)) ;
  (* XXX(dinosaure): PACK file + the hash of the commit. The hash of the commit
     is not really needed if we assert that we store only one commit finally. We
     can just, for the decoding, unpack everything and find the only commit
     available into the PACK file. *)
  stream None ;
  Lwt.return_unit

let to_octets t =
  (* TODO maybe preserve edn and branch as well? *)
  let open Lwt.Infix in
  match t.head with
  | None -> Lwt.return ""
  | Some head ->
    Store.read_exn t.store head >|= function
    | Commit c ->
      let l = Encore.to_lavoisier Git_commit.format in
      Encore.Lavoisier.emit_string c l
    | _ -> assert false

let of_octets ctx ~remote data =
  (* TODO maybe recover edn and branch from data as well? *)
  let open Lwt_result.Infix in
  let l = Encore.to_angstrom Git_commit.format in
  Lwt.return
    (Result.map_error (fun e -> `Msg e)
       (Angstrom.parse_string ~consume:All l data)) >>= fun head ->
  let head = Git_commit.tree head in
  init_store () >|= fun store ->
  let edn, branch = split_url remote in
  { ctx ; edn ; branch ; store ; head = Some head }

let exists t key =
  let open Lwt.Infix in
  match t.head with
  | None -> Lwt.return (Ok None)
  | Some head ->
    Search.mem t.store head (`Commit (`Path (Mirage_kv.Key.segments key))) >>= function
    | false -> Lwt.return (Ok None)
    | true ->
      Search.find t.store head (`Commit (`Path (Mirage_kv.Key.segments key)))
      >|= Option.get >>= Store.read_exn t.store >>= function
      | Blob _ -> Lwt.return (Ok (Some `Value))
      | Tree _ | Commit _ | Tag _ -> Lwt.return (Ok (Some `Dictionary))

let get t key =
  let open Lwt.Infix in
  match t.head with
  | None -> Lwt.return (Error (`Not_found key))
  | Some head ->
    Search.find t.store head (`Commit (`Path (Mirage_kv.Key.segments key))) >>= function
    | None -> Lwt.return (Error (`Not_found key))
    | Some blob ->
      Store.read_exn t.store blob >|= function
      | Blob b -> Ok (Git.Blob.to_string b)
      | _ -> Error (`Value_expected key)

let get_partial t key ~offset ~length =
  let open Lwt_result.Infix in
  get t key >|= fun data ->
  if String.length data < offset then
    ""
  else
    let l = min length (String.length data - offset) in
    String.sub data offset l

let list t key =
  let open Lwt.Infix in
  match t.head with
  | None -> Lwt.return (Error (`Not_found key))
  | Some head ->
    Search.find t.store head (`Commit (`Path (Mirage_kv.Key.segments key))) >>= function
    | None -> Lwt.return (Error (`Not_found key))
    | Some tree ->
      Store.read_exn t.store tree >>= function
      | Tree t ->
        Lwt_list.map_p (fun { Git.Tree.perm; name; _ } -> match perm with
          | `Commit | `Dir -> Lwt.return (name, `Dictionary)
          | `Everybody | `Exec | `Normal -> Lwt.return (name, `Value)
          | `Link -> failwith "Unimplemented link follow")
        (Store.Value.Tree.to_list t) >|= Result.ok
      | _ -> Lwt.return (Error (`Dictionary_expected key))

let last_modified t key =
  let open Lwt.Infix in
  Option.fold
    ~none:(Lwt.return (Error (`Not_found key)))
    ~some:(fun head ->
        Store.read_exn t.store head >|= function
        | Commit c ->
          let author = Git_commit.author c in
          let secs, tz_offset = author.Git.User.date in
          let secs =
            Option.fold ~none:secs
              ~some:(fun { Git.User.sign ; hours ; minutes } ->
                  let tz_off = Int64.(mul (add (mul (of_int hours) 60L) (of_int minutes)) 60L) in
                  match sign with
                  | `Plus -> Int64.(sub secs tz_off)
                  | `Minus -> Int64.(add secs tz_off))
              tz_offset
          in
          let ts =
            Option.fold ~none:Ptime.epoch ~some:Fun.id (Ptime.of_float_s (Int64.to_float secs))
          in
          Ok (Ptime.(Span.to_d_ps (to_span ts)))
        | _ -> assert false)
    t.head

let digest t key =
  Option.fold
    ~none:(Error (`Not_found key))
    ~some:(fun x -> Ok (Store.Hash.to_hex x))
    t.head |> Lwt.return

let size t key =
  let open Lwt_result.Infix in
  get t key >|= fun data ->
  String.length data
