module Store = Git.Mem.Make(Digestif.SHA1)
module Sync = Git.Mem.Sync(Store)
module Search = Git.Search.Make(Digestif.SHA1)(Store)
module Git_commit = Git.Commit.Make(Store.Hash)

type t =
  { ctx : Mimic.ctx
  ; edn : Smart_git.Endpoint.t
  ; branch : Git.Reference.t
  ; store : Store.t
  ; mutable committed : Digestif.SHA1.t option
  ; mutex : Lwt_mutex.t
  ; mutable head : Store.hash option }

let init_store () =
  let open Lwt.Infix in
  Store.v (Fpath.v ".") >|= fun r ->
  Result.map_error
    (fun e -> `Msg (Fmt.str "error setting up store %a" Store.pp_error e))
    r

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
    Smart_git.Endpoint.of_string s |> to_invalid, Git.Reference.main

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
  ( match t.head with
  | None -> Lwt.return (`Depth 1)
  | Some head ->
    Store.read_exn t.store head >>= fun value ->
    let[@warning "-8"] Git.Value.Commit commit = value in
    (* TODO(dinosaure): we should handle correctly [tz] and re-calculate the timestamp. *)
    let { Git.User.date= (timestamp, _tz); _ } = Store.Value.Commit.author commit in
    Lwt.return (`Timestamp timestamp) ) >>= fun deepen ->
  Sync.fetch ~capabilities ~ctx:t.ctx t.edn t.store ~deepen (`Some [ t.branch, t.branch ]) >>= fun r ->
  let data =
    Result.map_error
      (fun e -> `Msg (Fmt.str "error fetching: %a" Sync.pp_error e))
      r
  in
  match data with
  | Error _ as e -> Lwt.return e
  | Ok None -> Lwt.return (Ok [])
  | Ok Some (_, refs) -> match List.find (fun (r, _) -> Git.Reference.equal r t.branch) refs with
    | (_, head) ->
      Store.shallow t.store head >>= fun () ->
      (* XXX(dinosaure): the shallow must be done **before** the diff. Otherwise
         we will compare [commit0] with [commit0 <- commit1]. We want to compare
         [commit0] and [commit1] (only). *)
      diff t.store t.head head >>= fun diff ->
      t.head <- Some head ; Lwt.return (Ok diff)
    | exception Not_found -> Lwt.return_error (`Msg (Fmt.str "error fetching: %a does not exist" Git.Reference.pp t.branch))

let connect ctx endpoint =
  let open Lwt.Infix in
  init_store () >>= fun store ->
  let store = to_invalid store in
  let edn, branch = split_url endpoint in
  let t = { ctx ; edn ; branch ; store ; committed= None; mutex= Lwt_mutex.create (); head= None } in
  pull t >>= fun r ->
  let _r = to_invalid r in
  Lwt.return t

let branch t = t.branch

let commit t =
  match t.head, t.committed with
  | None, _ -> None
  | Some commit, None -> Some (`Clean commit)
  | Some commit, Some _ ->
    (* XXX: this is not precise as we can have made zero changes *)
    Some (`Dirty commit)

type key = Mirage_kv.Key.t

type change = [
  | `Add of key
  | `Remove of key
  | `Change of key
]

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

let ( <.> ) f g = fun x -> f (g x)
let ( >>? ) x f = let open Lwt.Infix in match x with
  | Some x -> f x >>= fun v -> Lwt.return_some v
  | None -> Lwt.return_none
let ( >>! ) x f = Lwt.Infix.(x >>= f)

let pack t ?(level= 4) ~commit stream =
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
    Carton.Enc.target_patch targets.(idx)
    >>? (find <.> Carton.Enc.source_of_patch)
    >>= function
    | Some None -> failwith "Try to encode an OBJ_REF object" (* XXX(dinosaure): should never occur! *)
    | Some (Some (_ (* offset *) : int)) | None ->
      Hashtbl.add offsets (Carton.Enc.target_uid targets.(idx)) !cursor ;
      Carton_lwt.Enc.encode_target ~level ~b ~find ~load:(load t) ~uid targets.(idx) ~cursor:(Int64.to_int !cursor)
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
      Lwt.pause () >>= fun () -> go encoder in
  let rec go idx =
    if idx < Array.length targets
    then encode_target idx >>= fun () -> go (succ idx)
    else Lwt.pause () in
  go 0 >>= fun () ->
  let hash = SHA1.get !ctx in
  stream (Some (SHA1.to_raw_string hash)) ;
  stream None ;
  Lwt.return_unit

let to_octets ?level t = match t.head with
  | None ->
    let str = "PACK\000\000\000\002\000\000\000\000\
               \x02\x9d\x08\x82\x3b\xd8\xa8\xea\xb5\x10\xad\x6a\xc7\x5c\x82\x3c\xfd\x3e\xd3\x1e" in
    Lwt_stream.of_list [ str ]
  | Some commit ->
    let stream, push = Lwt_stream.create () in
    Lwt.async (fun () -> pack ?level t.store ~commit push);
    stream

(* XXX(dinosaure): we have the full-control between [to_octets]/[of_octets]
   and we are currently not able to generate a PACK file with OBJ_REF objects.
   That mostly means that only one pass is enough to extract all objects!
   OBJ_OFS objects need only already consumed objects. *)

let map buf ~pos len =
  let str = Buffer.contents buf in
  let off = Int64.to_int pos in
  let len = min (String.length str - off) len in
  Bigstringaf.of_string str ~off:(Int64.to_int pos) ~len

let blit_from_string src src_off dst dst_off len =
  Bigstringaf.blit_from_string src ~src_off dst ~dst_off ~len

let read ke stream =
  let rec go filled input =
    match Ke.Rke.N.peek ke with
    | [] -> begin
      let open Lwt.Infix in
      Lwt_stream.get stream >>= function
      | Some str ->
        Ke.Rke.N.push ke ~blit:blit_from_string ~length:String.length str;
        go filled input
      | None -> Lwt.return filled end
    | src :: _ ->
      let src = Cstruct.of_bigarray src in
      let len = min (Cstruct.length input) (Cstruct.length src) in
      Cstruct.blit src 0 input 0 len;
      Ke.Rke.N.shift_exn ke len;
      if len < Cstruct.length input
      then go (filled + len) (Cstruct.shift input len)
      else Lwt.return (filled + len) in
  fun input -> go 0 input

let analyze store stream =
  let tmp = Cstruct.create 0x1000 in
  let buf = Buffer.create 0x1000 in
  let ke = Ke.Rke.create ~capacity:0x1000 Bigarray.char in
  let read_cstruct tmp =
    let open Lwt.Infix in
    read ke stream tmp >>= fun len ->
    Buffer.add_string buf (Cstruct.to_string ~off:0 ~len tmp);
    Lwt.return len in
  let allocate bits = De.make_window ~bits in
  let never _ = assert false in
  let pack = Carton.Dec.make buf ~allocate
    ~z:(De.bigstring_create De.io_buffer_size)
    ~uid_ln:SHA1.length ~uid_rw:SHA1.of_raw_string never in
  let objects = Hashtbl.create 0x100 in

  let rec go head decoder = let open Lwt.Infix in
    match First_pass.decode decoder with
    | `Await decoder ->
      read_cstruct tmp >>= fun len ->
      go head (First_pass.src decoder (Cstruct.to_bigarray tmp) 0 len)
    | `Peek decoder ->
      let keep = First_pass.src_rem decoder in
      read_cstruct (Cstruct.shift tmp keep) >>= fun len ->
      go head (First_pass.src decoder (Cstruct.to_bigarray tmp) 0 (keep + len))
    | `Entry ({ First_pass.kind= Base _; offset= cursor; _ }, decoder) ->
      let weight = Carton.Dec.weight_of_offset ~map pack ~weight:Carton.Dec.null cursor in
      let raw = Carton.Dec.make_raw ~weight in
      let v = Carton.Dec.of_offset ~map pack raw ~cursor in
      Hashtbl.add objects cursor v ;
      let kind = match Carton.Dec.kind v with
        | `A -> `Commit
        | `B -> `Tree
        | `C -> `Blob
        | `D -> `Tag in
      Store.write_inflated store ~kind
        (Cstruct.of_bigarray ~off:0 ~len:(Carton.Dec.len v) (Carton.Dec.raw v)) >>= fun hash ->
      ( match kind with
      | `Commit -> go (Some hash) decoder
      | _ -> go head decoder )
    | `Entry ({ First_pass.kind= Ofs { sub= s; _ } ; offset= cursor; _ }, decoder) ->
      let weight = Carton.Dec.weight_of_offset ~map pack ~weight:Carton.Dec.null cursor in
      let source = Int64.sub cursor (Int64.of_int s) in
      let v = Carton.Dec.copy ~flip:true ~weight (Hashtbl.find objects source) (* XXX(dinosaure): should never fail *) in
      let v = Carton.Dec.of_offset_with_source ~map pack v ~cursor in
      Hashtbl.add objects cursor v ;
      let kind = match Carton.Dec.kind v with
        | `A -> `Commit
        | `B -> `Tree
        | `C -> `Blob
        | `D -> `Tag in
      Store.write_inflated store ~kind
        (Cstruct.of_bigarray ~off:0 ~len:(Carton.Dec.len v) (Carton.Dec.raw v)) >>= fun hash ->
      ( match kind with
      | `Commit -> go (Some hash) decoder
      | _ -> go head decoder )
    | `Entry ({ First_pass.kind= Ref _; _ }, _decoder) ->
      failwith "Invalid PACK file (OBJ_REF)"
    | `End _hash -> Lwt.return head
    | `Malformed err -> failwith err in
  let decoder = First_pass.decoder ~o:(Bigstringaf.create De.io_buffer_size) ~allocate `Manual in
  go None decoder

let of_octets ctx ~remote data =
  let open Lwt.Infix in
  (* TODO maybe recover edn and branch from data as well? *)
  Lwt.catch
    (fun () ->
       init_store () >|=
       Result.fold ~ok:Fun.id ~error:(function `Msg msg -> failwith msg) >>= fun store ->
       analyze store data >>= fun head ->
       let edn, branch = split_url remote in
       Lwt.return_ok { ctx ; edn ; branch ; store ; committed= None; mutex= Lwt_mutex.create (); head; })
    (fun _exn ->
       Lwt.return_error (`Msg "Invalid PACK file"))

module Make (Pclock : Mirage_clock.PCLOCK) = struct
  type nonrec t = t
  type key = Mirage_kv.Key.t

  type error = [ `Msg of string | Mirage_kv.error ]
  type write_error = [ `Msg of string
                     | `Hash_not_found of Digestif.SHA1.t
                     | `Reference_not_found of Git.Reference.t
                     | Mirage_kv.write_error ]

  let pp_error ppf = function
    | #Mirage_kv.error as err -> Mirage_kv.pp_error ppf err
    | `Msg msg -> Fmt.string ppf msg

  let disconnect _t = Lwt.return_unit

  let pp_write_error ppf = function
    | #Mirage_kv.write_error as err -> Mirage_kv.pp_write_error ppf err
    | `Reference_not_found _ | `Msg _ as err -> Store.pp_error ppf err
    | `Hash_not_found hash -> Store.pp_error ppf (`Not_found hash)

  let now () = Int64.of_float (Ptime.to_float_s (Ptime.v (Pclock.now_d_ps ())))

  let find_blob t key =
    match t.committed, t.head with
    | None, None -> Lwt.return None
    | Some tree_root_hash, _ ->
      Search.find t.store tree_root_hash (`Path (Mirage_kv.Key.segments key))
    | None, Some commit ->
      Search.find t.store commit (`Commit (`Path (Mirage_kv.Key.segments key)))

  let exists t key =
    let open Lwt.Infix in
    find_blob t key >>= function
    | None -> Lwt.return (Ok None)
    | Some tree_hash ->
      Store.read_exn t.store tree_hash >>= function
        | Blob _ -> Lwt.return (Ok (Some `Value))
        | Tree _ | Commit _ | Tag _ -> Lwt.return (Ok (Some `Dictionary))

  let get t key =
    let open Lwt.Infix in
    find_blob t key >>= function
      | None -> Lwt.return (Error (`Not_found key))
      | Some blob ->
        Store.read_exn t.store blob >|= function
        | Blob b -> Ok (Git.Blob.to_string b)
        | _ -> Error (`Value_expected key)

  let get_partial t key ~offset ~length =
    let open Lwt_result.Infix in
    get t key >>= fun data ->
    let off = Optint.Int63.to_int offset in
    if off < 0 then
      Lwt_result.fail (`Msg "offset does not fit into integer")
    else if String.length data < off then
      Lwt_result.return ""
    else
      let l = min length (String.length data - off) in
      Lwt_result.return (String.sub data off l)

  let list t key =
    let open Lwt.Infix in
    find_blob t key >>= function
      | None -> Lwt.return (Error (`Not_found key))
      | Some tree ->
        Store.read_exn t.store tree >>= function
        | Tree t ->
          let r =
            List.map (fun { Git.Tree.perm; name; _ } ->
                Mirage_kv.Key.add key name,
                match perm with
                | `Commit | `Dir -> `Dictionary
                | `Everybody | `Exec | `Normal | `Link -> `Value)
              (Store.Value.Tree.to_list t)
          in
          Lwt.return (Ok r)
        | _ -> Lwt.return (Error (`Dictionary_expected key))

  let last_modified t key =
    let open Lwt.Infix in
    find_blob t key >>=
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
              Option.fold
                ~none:Ptime.epoch
                ~some:Fun.id (Ptime.of_float_s (Int64.to_float secs))
            in
            Ok ts
          | _ ->
            Ok (Option.fold
              ~none:Ptime.epoch
              ~some:Fun.id (Ptime.of_float_s (Int64.to_float (now ())))))

  let digest t key =
    let open Lwt.Infix in
    find_blob t key >>=
    Option.fold
      ~none:(Lwt.return (Error (`Not_found key)))
      ~some:(fun x -> Lwt.return (Ok (Store.Hash.to_raw_string x)))

  let size t key =
    let open Lwt_result.Infix in
    get t key >|= fun data ->
    Optint.Int63.of_int (String.length data)

  let author ?(name = "Git KV") ?(email = "git-noreply@robur.coop") now =
    { Git.User.name ; email ; date = now (), None }

  let rec unroll_tree t ~tree_root_hash (pred_perm, pred_name, pred_hash) rpath =
    let open Lwt.Infix in
    let ( >>? ) = Lwt_result.bind in
    match rpath with
    | [] ->
      ( Store.read_exn t.store tree_root_hash >>= function
      | Git.Value.Tree tree ->
        let tree = Git.Tree.(add (entry ~name:pred_name pred_perm pred_hash) (remove ~name:pred_name tree)) in
        Store.write t.store (Git.Value.Tree tree) >>? fun (hash, _) -> Lwt.return_ok hash
      | _ -> assert false )
    | name :: rest ->
      Search.find t.store tree_root_hash (`Path (List.rev rpath)) >>= function
      | None ->
        let tree = Git.Tree.(v [ entry ~name:pred_name pred_perm pred_hash ]) in
        Store.write t.store (Git.Value.Tree tree) >>? fun (hash, _) ->
        unroll_tree t ~tree_root_hash (`Dir, name, hash) rest
      | Some tree_hash ->
        ( Store.read_exn t.store tree_hash >>= function
        | Git.Value.Tree tree ->
          let tree = Git.Tree.(add (entry ~name:pred_name pred_perm pred_hash) (remove ~name:pred_name tree)) in
          Store.write t.store (Git.Value.Tree tree) >>? fun (hash, _) ->
          unroll_tree t ~tree_root_hash (`Dir, name, hash) rest
          | _ -> assert false )

  let tree_root_hash_of_store t =
    match t.committed, t.head with
    | Some tree_root_hash, _ -> Lwt.return_ok tree_root_hash
    | None, None ->
      let open Lwt_result.Infix in
      let tree = Store.Value.Tree.v [] in
      Store.write t.store (Git.Value.Tree tree) >>= fun (hash, _) ->
      Lwt.return_ok hash
    | None, Some commit ->
      let open Lwt.Infix in
      Store.read_exn t.store commit >>= function
      | Git.Value.Commit commit -> Lwt.return_ok (Store.Value.Commit.tree commit)
      | _ -> Lwt.return_error (`Msg (Fmt.str "The current HEAD value (%a) is not a commit" Digestif.SHA1.pp commit))

  let ( >>? ) = Lwt_result.bind

  let set ?and_commit t key contents =
    let segs = Mirage_kv.Key.segments key in
    match segs with
    | [] -> assert false (* TODO *)
    | path ->
      let blob = Git.Blob.of_string contents in
      let rpath = List.rev path in
      let name = List.hd rpath in
      let open Lwt_result.Infix in
      Store.write t.store (Git.Value.Blob blob) >>= fun (hash, _) ->
      tree_root_hash_of_store t >>= fun tree_root_hash ->
      unroll_tree t ~tree_root_hash (`Normal, name, hash) (List.tl rpath) >>= fun tree_root_hash ->
      match and_commit with
      | Some _old_tree_root_hash ->
        t.committed <- Some tree_root_hash ;
        Lwt.return_ok ()
      | None ->
        let committer = author now in
        let author    = author now in
        let action    = Option.fold ~none:(`Create t.branch) ~some:(fun _ -> `Update (t.branch, t.branch)) t.head in
        let parents   = Option.to_list t.head in
        let commit    = Store.Value.Commit.make ~tree:tree_root_hash ~author ~committer
          ~parents (Some "Committed by git-kv") in
        Store.write t.store (Git.Value.Commit commit) >>= fun (hash, _) ->
        Store.Ref.write t.store t.branch (Git.Reference.uid hash) >>= fun () ->
        Lwt.Infix.(Sync.push ~capabilities ~ctx:t.ctx t.edn t.store [ action ]
          >|= Result.map_error (fun err -> `Msg (Fmt.str "error pushing branch %a: %a"
            Git.Reference.pp t.branch Sync.pp_error err))
          >>? fun () -> Store.shallow t.store hash >|= Result.ok) >>= fun () ->
        t.head <- Some hash ; Lwt.return_ok ()

  let to_write_error (error : Store.error) = match error with
    | `Not_found hash -> `Hash_not_found hash
    | `Reference_not_found ref -> `Reference_not_found ref
    | `Msg err -> `Msg err
    | err -> `Msg (Fmt.to_to_string Store.pp_error err)

  let set t key contents =
    let open Lwt.Infix in
    set ?and_commit:t.committed t key contents
    >|= Result.map_error to_write_error

  let set_partial t key ~offset chunk =
    let open Lwt_result.Infix in
    get t key >>= fun contents ->
    let len = String.length contents in
    let add = String.length chunk in
    let off = Optint.Int63.to_int offset in
    if off < 0 then
      Lwt_result.fail (`Msg "offset does not fit into integer")
    else
      let res = Bytes.make (max len (off + add)) '\000' in
      Bytes.blit_string contents 0 res 0 len ;
      Bytes.blit_string chunk 0 res off add ;
      set t key (Bytes.unsafe_to_string res)

  let remove ?and_commit t key =
    let segs = Mirage_kv.Key.segments key in
    match List.rev segs with
    | [] -> assert false
    | name :: [] ->
      let open Lwt_result.Infix in
      tree_root_hash_of_store t >>= fun tree_root_hash ->
      Store.read_exn t.store tree_root_hash >>! fun tree_root ->
      let[@warning "-8"] Git.Value.Tree tree_root = tree_root in
      let tree_root = Git.Tree.remove ~name tree_root in
      let open Lwt_result.Infix in
      Store.write t.store (Git.Value.Tree tree_root) >>= fun (tree_root_hash, _) ->
      ( match and_commit with
      | Some _old_tree_root_hash -> t.committed <- Some tree_root_hash ; Lwt.return_ok ()
      | None ->
        let committer = author now in
        let author    = author now in
        let parents = Option.to_list t.head in
        let commit    = Store.Value.Commit.make ~tree:tree_root_hash ~author ~committer
          ~parents (Some "Committed by git-kv") in
        Store.write t.store (Git.Value.Commit commit) >>= fun (hash, _) ->
        Store.Ref.write t.store t.branch (Git.Reference.uid hash) >>= fun () ->
        Lwt.Infix.(Sync.push ~capabilities ~ctx:t.ctx t.edn t.store [ `Update (t.branch, t.branch) ]
          >|= Result.map_error (fun err -> `Msg (Fmt.str "error pushing branch %a: %a"
            Git.Reference.pp t.branch Sync.pp_error err))
          >>? fun () -> Store.shallow t.store hash >|= Result.ok)
        >>= fun () -> t.head <- Some hash ; Lwt.return_ok () )
    | name :: pred_name :: rest ->
      let open Lwt_result.Infix in
      tree_root_hash_of_store t >>= fun tree_root_hash ->
      Search.find t.store tree_root_hash (`Path (List.rev (pred_name :: rest))) >>! function
      | None -> Lwt.return_ok ()
      | Some hash -> Store.read_exn t.store hash >>! function
        | Git.Value.Tree tree ->
          let tree = Git.Tree.remove ~name tree in
          Store.write t.store (Git.Value.Tree tree) >>= fun (pred_hash, _) ->
          unroll_tree t ~tree_root_hash (`Dir, pred_name, pred_hash) rest >>= fun tree_root_hash ->
          ( match and_commit with
          | Some _old_tree_root_hash -> t.committed <- Some tree_root_hash ; Lwt.return_ok ()
          | None ->
            let committer = author now in
            let author    = author now in
            let parents = Option.to_list t.head in
            let commit    = Store.Value.Commit.make ~tree:tree_root_hash ~author ~committer
              ~parents (Some "Committed by git-kv") in
            Store.write t.store (Git.Value.Commit commit) >>= fun (hash, _) ->
            Store.Ref.write t.store t.branch (Git.Reference.uid hash) >>= fun () ->
            Lwt.Infix.(Sync.push ~capabilities ~ctx:t.ctx t.edn t.store [ `Update (t.branch, t.branch) ]
              >|= Result.map_error (fun err -> `Msg (Fmt.str "error pushing branch %a: %a"
                Git.Reference.pp t.branch Sync.pp_error err))
              >>? fun () -> Store.shallow t.store hash >|= Result.ok)
            >>= fun () -> t.head <- Some hash ; Lwt.return_ok () )
        | _ -> Lwt.return_ok ()

  let remove t key =
    let open Lwt.Infix in
    remove ?and_commit:t.committed t key >|= Result.map_error to_write_error

  let allocate t key ?last_modified:_ size =
    let open Lwt.Infix in
    exists t key >>= function
    | Error _ as e -> Lwt.return e
    | Ok Some _ -> Lwt_result.fail (`Already_present key)
    | Ok None ->
      let size = Optint.Int63.to_int size in
      if size < 0 then
        Lwt_result.fail (`Msg "size does not fit into integer")
      else
        let data = String.make size '\000' in
        set t key data

  let change_and_push t ?author:name ?author_email:email ?(message = "Committed by git-kv") f =
    let open Lwt.Infix in
    match t.committed with
    | Some _ -> Lwt.return_error (`Msg "Nested change_and_push")
    | None ->
      Lwt_mutex.with_lock t.mutex (fun () ->
          (let open Lwt_result.Infix in
           tree_root_hash_of_store t >>= fun tree_root_hash ->
           let t' = { t with committed = Some tree_root_hash } in
           f t' >>! fun res ->
           (* XXX(dinosaure): we assume that only [change_and_push] can reset [t.committed] to [None] and
              we ensured that [change_and_push] can not be called into [f]. So we are sure that [t'.committed]
              must be [Some _] in anyway. *)
           let[@warning "-8"] Some new_tree_root_hash = t'.committed in
           if Digestif.SHA1.equal new_tree_root_hash tree_root_hash
           then Lwt.return_ok res (* XXX(dinosaure): nothing to send! *)
           else if not (Option.equal Digestif.SHA1.equal t.head t'.head) then
             Lwt.return (Error (`Msg "store was modified outside of change_and_push, please retry"))
           else
             let action    = Option.fold ~none:(`Create t.branch) ~some:(fun _ -> `Update (t.branch, t.branch)) t.head in
             let parents   = Option.to_list t.head in
             let author    = author ?name ?email now in
             let committer = author in
             let commit    = Store.Value.Commit.make ~tree:new_tree_root_hash ~author ~committer
                 ~parents (Some message) in
             Store.write t.store (Git.Value.Commit commit) >>= fun (hash, _) ->
             Store.Ref.write t.store t.branch (Git.Reference.uid hash) >>= fun () ->
             Lwt.Infix.(Sync.push ~capabilities ~ctx:t.ctx t.edn t.store [ action ]
                        >|= Result.map_error (fun err ->
                            `Msg (Fmt.str "error pushing branch %a: %a"
                                    Git.Reference.pp t.branch Sync.pp_error err))
                        >>? fun () ->
                        Store.shallow t.store hash >|= Result.ok) >>= fun () ->
             t.head <- Some hash ;
             Lwt.return_ok res)
          >|= Result.map_error
            (fun err -> `Msg (Fmt.str "error pushing %a" Store.pp_error err)))

  let rename t ~source ~dest =
    let open Lwt_result.Infix in
    let op t =
      get t source >>= fun contents ->
      remove t source >>= fun () ->
      set t dest contents
    in
    (* (hannes) we check whether we're in a change_and_push or not, since
       nested change_and_push are not supported. *)
    match t.committed with
    | Some _ -> op t
    | None ->
      change_and_push t op >>! function
      | Ok a -> Lwt.return a
      | Error _ as e -> Lwt.return e
end
