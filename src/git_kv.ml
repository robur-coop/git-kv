
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
  if Fpath.equal root v
  then Mirage_kv.Key.empty
  else Mirage_kv.Key.v (Fpath.to_string (Option.get (Fpath.relativize ~root v)))

let diff store commit0 commit1 =
  let open Lwt.Infix in
  let root = Fpath.v "./" in
  let tbl0 = Hashtbl.create 0x10 in
  Store.fold store (fun () ?name ~length:_ hash _value ->
    Option.iter (fun name -> Hashtbl.add tbl0 (fpath_to_key ~root name) hash) name ;
    Lwt.return ()) () ~path:root commit0 >>= fun () ->
  let tbl1 = Hashtbl.create 0x10 in
  Store.fold store (fun () ?name ~length:_ hash _value ->
    Option.iter (fun name -> Hashtbl.add tbl1 (fpath_to_key ~root name) hash) name ;
    Lwt.return ()) () ~path:root commit1 >>= fun () ->
  let diff = Hashtbl.fold (fun name hash diff ->
    match Hashtbl.find_opt tbl1 name with
    | Some hash' when not (Digestif.SHA1.equal hash hash') -> `Change name :: diff
    | Some _ -> diff
    | None -> `Remove name :: diff) tbl0 [] in
  let diff = Hashtbl.fold (fun name _hash diff ->
    if not (Hashtbl.mem tbl0 name)
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
  let feed ctx ?off ?len ba = feed_bigstring ctx ?off ?len ba
  let null = digest_string ""
  let length = digest_size
  let compare a b =
    String.compare
      (to_raw_string a) (to_raw_string b)
end

module Verbose = struct
  type 'a fiber = 'a Lwt.t

  let succ _ = Lwt.return_unit
  let print _ = Lwt.return_unit
end

module Lwt_scheduler = struct
  module Mutex = struct
    type 'a fiber = 'a Lwt.t
    type t = Lwt_mutex.t

    let create () = Lwt_mutex.create ()
    let lock t = Lwt_mutex.lock t
    let unlock t = Lwt_mutex.unlock t
  end

  module Condition = struct
    type 'a fiber = 'a Lwt.t
    type mutex = Mutex.t
    type t = unit Lwt_condition.t

    let create () = Lwt_condition.create ()
    let wait t mutex = Lwt_condition.wait ~mutex t
    let signal t = Lwt_condition.signal t ()
    let broadcast t = Lwt_condition.broadcast t ()
  end

  type 'a t = 'a Lwt.t

  let bind x f = Lwt.bind x f
  let return x = Lwt.return x
  let parallel_map ~f lst = Lwt_list.map_p f lst
  let parallel_iter ~f lst = Lwt_list.iter_p f lst
  let detach f =
    let th, wk = Lwt.wait () in
    Lwt.async (fun () ->
      let res = f () in
      Lwt.wakeup_later wk res ;
      Lwt.return_unit) ;
   th
end

module Scheduler = Carton.Make (Lwt)
module Delta = Carton_lwt.Enc.Delta (SHA1) (Verbose)
module First_pass = Carton.Dec.Fp (SHA1)
module Verify = Carton.Dec.Verify (SHA1) (Scheduler) (Lwt_scheduler)

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
  stream None ;
  Lwt.return_unit

let to_octets t = match t.head with
  | None -> assert false (* TODO(dinosaure): empty PACK file *)
  | Some commit ->
    let buf = Buffer.create 0x100 in
    let stream = Option.iter (Buffer.add_string buf) in
    let open Lwt.Infix in
    pack t.store ~commit stream >|= fun () ->
    Buffer.contents buf

let digest ~kind ?(off = 0) ?len buf =
  let len =
    match len with Some len -> len | None -> Bigstringaf.length buf - off in
  let ctx = SHA1.empty in
  let ctx =
    match kind with
    | `A -> SHA1.feed_string ctx (Fmt.str "commit %d\000" len)
    | `B -> SHA1.feed_string ctx (Fmt.str "tree %d\000" len)
    | `C -> SHA1.feed_string ctx (Fmt.str "blob %d\000" len)
    | `D -> SHA1.feed_string ctx (Fmt.str "tag %d\000" len) in
  let ctx = SHA1.feed_bigstring ctx ~off ~len buf in
  SHA1.get ctx

let analyze stream =
  let where = Hashtbl.create 0x100 in
  let child = Hashtbl.create 0x100 in
  let sizes = Hashtbl.create 0x100 in

  let replace tbl k v = match Hashtbl.find_opt tbl k with
    | Some v' -> if v' < v then Hashtbl.replace tbl k v
    | _ -> Hashtbl.add tbl k v in

  let rec go acc tmp decoder = let open Lwt.Infix in
    match First_pass.decode decoder with
    | `Await decoder ->
      ( stream () >>= function
      | Some str ->
        let tmp = Bigstringaf.of_string str ~off:0 ~len:(String.length str) in
        go acc tmp (First_pass.src decoder tmp 0 (String.length str))
      | None -> failwith "Truncated PACK file" )
    | `Peek decoder ->
      let keep = First_pass.src_rem decoder in
      ( stream () >>= function
      | Some str ->
        let tmp = Bigstringaf.create (keep + String.length str) in
        Bigstringaf.blit tmp ~src_off:0 tmp ~dst_off:0 ~len:keep ;
        Bigstringaf.blit_from_string str ~src_off:0 tmp ~dst_off:keep 
          ~len:(String.length str) ;
        go acc tmp (First_pass.src decoder tmp 0 (keep + String.length str))
      | None -> failwith "Truncated PACK file" )
    | `Entry ({ First_pass.kind= Base _; offset; size; _ }, decoder) ->
      Hashtbl.add where offset (First_pass.count decoder - 1) ;
      Hashtbl.add sizes offset size ;
      go (Verify.unresolved_base ~cursor:offset :: acc) tmp decoder
    | `Entry ({ First_pass.kind= Ofs { sub= v; source; target; }
              ; offset; _ }, decoder) ->
      Hashtbl.add where offset (First_pass.count decoder - 1) ;
      replace sizes Int64.(sub offset (of_int v)) source ;
      replace sizes offset target ;
      ( try let vs = Hashtbl.find child (`Ofs Int64.(sub offset (of_int v))) in
            Hashtbl.replace child (`Ofs Int64.(sub offset (of_int v))) (offset :: vs)
        with _ -> Hashtbl.add child (`Ofs Int64.(sub offset (of_int v))) [ offset ] ) ;
      go (Verify.unresolved_node :: acc) tmp decoder
    | `Entry ({ First_pass.kind= Ref { ptr; target; source; }
              ; offset; _ }, decoder) ->
      Hashtbl.add where offset (First_pass.count decoder - 1) ;
      replace sizes offset (Stdlib.max target source) ;
      ( try let vs = Hashtbl.find child (`Ref ptr) in
            Hashtbl.replace child (`Ref ptr) (offset :: vs)
        with _ -> Hashtbl.add child (`Ref ptr) [ offset ] ) ;
      go (Verify.unresolved_node :: acc) tmp decoder
    | `End _hash ->
      let where ~cursor = Hashtbl.find where cursor in
      let children ~cursor ~uid =
        match Hashtbl.find_opt child (`Ofs cursor),
              Hashtbl.find_opt child (`Ref uid) with
        | Some a, Some b -> List.sort_uniq compare (a @ b)
        | Some x, None | None, Some x -> x
        | None, None -> [] in
      let weight ~cursor = Hashtbl.find sizes cursor in
      let oracle = { Carton.Dec.where
                   ; Carton.Dec.children
                   ; Carton.Dec.digest
                   ; Carton.Dec.weight } in
      Lwt.return (List.rev acc, oracle)
    | `Malformed err -> failwith err in

  let o = Bigstringaf.create De.io_buffer_size in
  let allocate _ = De.make_window ~bits:15 in
  let decoder = First_pass.decoder ~o ~allocate `Manual in
  let open Lwt.Infix in
  go [] De.bigstring_empty decoder >>= fun (matrix, oracle) ->
  Lwt.return (Array.of_list matrix, oracle)

let stream_of_string str =
  let closed = ref false in
  fun () -> match !closed with
  | true -> Lwt.return_none
  | false -> closed := true ; Lwt.return_some str

let map contents ~pos len =
  let off = Int64.to_int pos in
  let len = min (String.length contents - off) len in
  Bigstringaf.of_string ~off:(Int64.to_int pos) ~len contents

let unpack contents =
  let open Lwt.Infix in
  analyze (stream_of_string contents) >>= fun (matrix, oracle) ->
  let z = De.bigstring_create De.io_buffer_size in
  let allocate bits = De.make_window ~bits in
  let never _ = assert false in
  let pack = Carton.Dec.make contents ~allocate ~z ~uid_ln:SHA1.length
    ~uid_rw:SHA1.of_raw_string never in
  Verify.verify ~threads:4 pack ~map ~oracle ~verbose:ignore ~matrix >>= fun () ->
  let index = Hashtbl.create (Array.length matrix) in
  let iter v =
    let offset = Verify.offset_of_status v in
    let hash = Verify.uid_of_status v in
    Hashtbl.add index hash offset in
  Array.iter iter matrix ;
  let pack =
    Carton.Dec.make contents ~allocate ~z ~uid_ln:SHA1.length
      ~uid_rw:SHA1.of_raw_string (Hashtbl.find index) in
  init_store ()
  >|= Rresult.R.reword_error (Rresult.R.msgf "%a" Store.pp_error)
  >|= Rresult.R.failwith_error_msg >>= fun store ->
  let rec go commit idx =
    if idx < Array.length matrix
    then
      let cursor = Verify.offset_of_status matrix.(idx) in
      let weight = Carton.Dec.weight_of_offset ~map pack ~weight:Carton.Dec.null cursor in
      let raw = Carton.Dec.make_raw ~weight in
      let v = Carton.Dec.of_offset ~map pack raw ~cursor in
      let kind = match Carton.Dec.kind v with
        | `A -> `Commit
        | `B -> `Tree
        | `C -> `Blob
        | `D -> `Tag in
      Store.write_inflated store ~kind
        (Cstruct.of_bigarray ~off:0 ~len:(Carton.Dec.len v) (Carton.Dec.raw v)) >>= fun hash ->
      ( if kind = `Commit
        then Store.shallow store hash
        else Lwt.return_unit ) >>= fun () ->
      go (if kind = `Commit then Some hash else None) (succ idx)
    else Lwt.return commit in
  go None 0 >>= fun head -> Lwt.return (store, head)

let of_octets ctx ~remote data =
  (* TODO maybe recover edn and branch from data as well? *)
  let open Lwt.Infix in
  Lwt.catch
    (fun () ->
       unpack data >>= fun (store, head) ->
       let edn, branch = split_url remote in
       Lwt.return_ok { ctx ; edn ; branch ; store ; head; })
    (fun exn ->
       Fmt.epr ">>> Got an exception: %s.\n%!" (Printexc.to_string exn) ;
       Fmt.epr ">>> %s.\n%!"
         (Printexc.raw_backtrace_to_string (Printexc.get_raw_backtrace ())) ;
       Lwt.return_error (`Msg "Invalid PACK file"))

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

let _get_partial t key ~offset ~length =
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

let _size t key =
  let open Lwt_result.Infix in
  get t key >|= fun data ->
  String.length data
