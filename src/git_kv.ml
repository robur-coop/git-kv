let src = Logs.Src.create "git-kv"

module Log = (val Logs.src_log src : Logs.LOG)

type t = {
  ctx: Mimic.ctx;
  edn: Git_store.Endpoint.t;
  branch: Git_store.Reference.t;
  store: Git_store.t;
  mutable committed: Digestif.SHA1.t option;
  mutex: Lwt_mutex.t;
  mutable head: Digestif.SHA1.t option;
}

let init_store () =
  let open Lwt.Infix in
  Git_store.v (Fpath.v ".") >|= fun r ->
  Result.map_error
    (fun e -> `Msg (Fmt.str "error setting up store %a" Git_store.pp_error e))
    r

let capabilities =
  [`Side_band_64k; `Multi_ack_detailed; `Ofs_delta; `Thin_pack; `Report_status]

let to_invalid = function Ok x -> x | Error (`Msg m) -> invalid_arg m

let split_url s =
  match String.split_on_char '#' s with
  | [edn; branch] ->
    ( Git_store.Endpoint.of_string edn |> to_invalid,
      Git_store.Reference.of_string ("refs/heads/" ^ branch) |> to_invalid )
  | _ -> Git_store.Endpoint.of_string s |> to_invalid, Git_store.Reference.main

let fpath_to_key ~root v =
  if Fpath.equal root v then Mirage_kv.Key.empty
  else Mirage_kv.Key.v (Fpath.to_string (Option.get (Fpath.relativize ~root v)))

let diff store commit0 commit1 =
  let open Lwt.Infix in
  let root = Fpath.v "./" in
  let tbl0 = Hashtbl.create 0x10 in
  Git_store.fold store
    (fun () ?name ~length:_ hash _value ->
      Option.iter
        (fun name -> Hashtbl.add tbl0 (fpath_to_key ~root name) hash)
        name;
      Lwt.return ())
    () ~path:root commit0
  >>= fun () ->
  let tbl1 = Hashtbl.create 0x10 in
  Git_store.fold store
    (fun () ?name ~length:_ hash _value ->
      Option.iter
        (fun name -> Hashtbl.add tbl1 (fpath_to_key ~root name) hash)
        name;
      Lwt.return ())
    () ~path:root commit1
  >>= fun () ->
  let diff =
    Hashtbl.fold
      (fun name hash diff ->
        match Hashtbl.find_opt tbl1 name with
        | Some hash' when not (Digestif.SHA1.equal hash hash') ->
          `Change name :: diff
        | Some _ -> diff
        | None -> `Remove name :: diff)
      tbl0 []
  in
  let diff =
    Hashtbl.fold
      (fun name _hash diff ->
        if not (Hashtbl.mem tbl0 name) then `Add name :: diff else diff)
      tbl1 diff
  in
  Lwt.return diff

let diff store commit0 commit1 =
  match commit0 with
  | Some commit0 -> diff store commit0 commit1
  | None ->
    let root = Fpath.v "." in
    Git_store.fold store
      (fun diff ?name ~length:_ _hash _value ->
        match name with
        | None -> Lwt.return diff
        | Some name -> Lwt.return (`Add (fpath_to_key ~root name) :: diff))
      [] ~path:root commit1

let pull t =
  let open Lwt.Infix in
  (match t.head with
  | None -> Lwt.return (`Depth 1)
  | Some head ->
    let value = Git_store.read_exn t.store head in
    let[@warning "-8"] (Git_store.Object.Commit commit) = value in
    (* TODO(dinosaure): we should handle correctly [tz] and re-calculate the timestamp. *)
    let {Git_store.User.date= timestamp, _tz; _} =
      Git_store.Commit.author commit
    in
    Lwt.return (`Timestamp timestamp))
  >>= fun deepen ->
  Git_sync.fetch ~capabilities ~ctx:t.ctx t.edn t.store ~deepen
    (`Some [t.branch, t.branch])
  >>= fun r ->
  let data =
    Result.map_error
      (fun e -> `Msg (Fmt.str "error fetching: %a" Git_sync.pp_error e))
      r
  in
  match data with
  | Error _ as e -> Lwt.return e
  | Ok None -> Lwt.return (Ok [])
  | Ok (Some (_, refs)) -> (
    match
      List.find (fun (r, _) -> Git_store.Reference.equal r t.branch) refs
    with
    | _, head ->
      Git_store.shallow t.store head >>= fun () ->
      (* XXX(dinosaure): the shallow must be done **before** the diff. Otherwise
         we will compare [commit0] with [commit0 <- commit1]. We want to compare
         [commit0] and [commit1] (only). *)
      diff t.store t.head head >>= fun diff ->
      t.head <- Some head;
      Lwt.return (Ok diff)
    | exception Not_found ->
      Lwt.return_error
        (`Msg
           (Fmt.str "error fetching: %a does not exist" Git_store.Reference.pp
              t.branch)))

let connect ctx endpoint =
  let open Lwt.Infix in
  init_store () >>= fun store ->
  let store = to_invalid store in
  let edn, branch = split_url endpoint in
  let t =
    {
      ctx;
      edn;
      branch;
      store;
      committed= None;
      mutex= Lwt_mutex.create ();
      head= None;
    }
  in
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
type change = [ `Add of key | `Remove of key | `Change of key ]

module SHA1 = Digestif.SHA1
module Pate = Carton_git_lwt.Make (SHA1)

let pack t ?(level = 4) ~commit push =
  let open Lwt.Infix in
  Git_store.fold t.store
    (fun acc ?name:_ ~length hash value ->
      let kind =
        match value with
        | Git_store.Object.Commit _ -> `A
        | Git_store.Object.Tree _ -> `B
        | Git_store.Object.Blob _ -> `C
        | Git_store.Object.Tag _ -> `D
      in
      let uid = Carton.Uid.unsafe_of_string (SHA1.to_raw_string hash) in
      let length = Int64.to_int length in
      Cartonnage.Entry.make ~kind ~length uid () :: acc |> Lwt.return)
    ~path:(Fpath.v ".") [] commit
  >|= Array.of_list
  >>= fun entries ->
  let with_header = Array.length entries in
  let with_signature = SHA1.empty in
  let load (uid : Carton.Uid.t) () =
    let hash = SHA1.of_raw_string (uid :> string) in
    let obj = Git_store.read_exn t.store hash in
    let bstr = Git_store.Object.to_bstr obj in
    let kind =
      match obj with
      | Git_store.Object.Commit _ -> `A
      | Git_store.Object.Tree _ -> `B
      | Git_store.Object.Blob _ -> `C
      | Git_store.Object.Tag _ -> `D
    in
    Lwt.return (Carton.Value.make ~kind bstr)
  in
  let targets = Pate.delta ~load (Lwt_seq.of_list (Array.to_list entries)) in
  let out =
    Pate.to_pack ~with_header ~with_signature ~load ~level
      (Lwt_stream.of_lwt_seq targets)
  in
  Lwt_seq.iter (fun str -> push (Some str)) out >>= fun () ->
  push None; Lwt.return_unit

let to_octets ?level t =
  match t.head with
  | None ->
    let str =
      "PACK\000\000\000\002\000\000\000\000\x02\x9d\x08\x82\x3b\xd8\xa8\xea\xb5\x10\xad\x6a\xc7\x5c\x82\x3c\xfd\x3e\xd3\x1e"
    in
    Lwt_stream.of_list [str]
  | Some commit ->
    let stream, push = Lwt_stream.create () in
    Lwt.async (fun () -> pack ?level t ~commit push);
    stream

(* XXX(dinosaure): we have the full-control between [to_octets]/[of_octets]
   and we are currently not able to generate a PACK file with OBJ_REF objects.
   That mostly means that only one pass is enough to extract all objects!
   OBJ_OFS objects need only already consumed objects. *)

(*
let map buf ~pos len =
  let str = Buffer.contents buf in
  let off = Int64.to_int pos in
  let len = min (String.length str - off) len in
  Bigstringaf.of_string str ~off:(Int64.to_int pos) ~len
*)

let analyze store stream =
  let open Lwt.Infix in
  let cfg = Pate.config () in
  let buf = Bbuffer.create 0x7ff in
  let append str ~off ~len =
    Bbuffer.add_substring buf str off len;
    Lwt.return_unit
  in
  let map buf ~pos len =
    let len' = Int.min (Bbuffer.length buf - pos) len in
    let bstr = Bstr.create len in
    Bbuffer.blit buf pos bstr 0 len';
    Bstr.fill bstr ~off:len' ~len:(len - len') '\000';
    bstr
  in
  let cache = Cachet.make ~map buf in
  Pate.verify_from_stream ~cfg ~append cache stream >>= fun (entries, _hash) ->
  let head = ref None in
  let t = Pate.make cache in
  let fn = function
    | Carton.Unresolved_node _ | Unresolved_base _ -> assert false
    | Resolved_base {cursor; uid; _} | Resolved_node {cursor; uid; _} -> (
      let size = Carton.size_of_offset t ~cursor Carton.Size.zero in
      let blob = Carton.Blob.make ~size in
      let value = Carton.of_offset t ~cursor blob in
      let kind =
        match Carton.Value.kind value with
        | `A -> `Commit
        | `B -> `Tree
        | `C -> `Blob
        | `D -> `Tag
      in
      if kind = `Commit then head := Some (SHA1.of_raw_string (uid :> string));
      let len = Carton.Value.length value in
      let bstr = Carton.Value.bigstring value in
      let bstr = Bstr.sub bstr ~off:0 ~len in
      match Git_store.Object.of_bstr ~kind bstr with
      | Ok value ->
        let _ = Git_store.write store value in
        ()
      | Error _ -> ())
  in
  Array.iter fn entries;
  match !head with
  | Some hash as head -> Git_store.shallow store hash >|= fun () -> head
  | None -> Lwt.return_none

let of_octets ctx ~remote data =
  let open Lwt.Infix in
  (* TODO maybe recover edn and branch from data as well? *)
  Lwt.catch
    (fun () ->
      init_store ()
      >|= Result.fold ~ok:Fun.id ~error:(function `Msg msg -> failwith msg)
      >>= fun store ->
      analyze store data >>= fun head ->
      let edn, branch = split_url remote in
      Lwt.return_ok
        {
          ctx;
          edn;
          branch;
          store;
          committed= None;
          mutex= Lwt_mutex.create ();
          head;
        })
    (fun exn ->
      let msg = Fmt.str "Invalid PACK file: %s" (Printexc.to_string exn) in
      Lwt.return_error (`Msg msg))

type error = [ `Msg of string | Mirage_kv.error ]

type write_error =
  [ `Msg of string
  | `Hash_not_found of Digestif.SHA1.t
  | `Reference_not_found of Git_store.Reference.t
  | Mirage_kv.write_error ]

let pp_error ppf = function
  | #Mirage_kv.error as err -> Mirage_kv.pp_error ppf err
  | `Msg msg -> Fmt.string ppf msg

let disconnect _t = Lwt.return_unit

let pp_write_error ppf = function
  | #Mirage_kv.write_error as err -> Mirage_kv.pp_write_error ppf err
  | (`Reference_not_found _ | `Msg _) as err -> Git_store.pp_error ppf err
  | `Hash_not_found hash -> Git_store.pp_error ppf (`Not_found hash)

let now () = Int64.of_float (Ptime.to_float_s (Mirage_ptime.now ()))

let find_blob t key =
  match t.committed, t.head with
  | None, None -> Lwt.return None
  | Some tree_root_hash, _ ->
    Git_search.find t.store tree_root_hash (`Path (Mirage_kv.Key.segments key))
  | None, Some commit ->
    Git_search.find t.store commit
      (`Commit (`Path (Mirage_kv.Key.segments key)))

let exists t key =
  let open Lwt.Infix in
  find_blob t key >>= function
  | None -> Lwt.return (Ok None)
  | Some tree_hash -> begin
    match Git_store.read_exn t.store tree_hash with
    | Blob _ -> Lwt.return (Ok (Some `Value))
    | Tree _ | Commit _ | Tag _ -> Lwt.return (Ok (Some `Dictionary))
  end

let get t key =
  let open Lwt.Infix in
  find_blob t key >>= function
  | None -> Lwt.return (Error (`Not_found key))
  | Some blob -> begin
    match Git_store.read_exn t.store blob with
    | Blob b -> Lwt.return_ok (Git_store.Blob.to_string b)
    | _ -> Lwt.return_error (`Value_expected key)
  end

let get_partial t key ~offset ~length =
  let open Lwt_result.Infix in
  get t key >>= fun data ->
  let off = Optint.Int63.to_int offset in
  if off < 0 then Lwt_result.fail (`Msg "offset does not fit into integer")
  else if String.length data < off then Lwt_result.return ""
  else
    let l = min length (String.length data - off) in
    Lwt_result.return (String.sub data off l)

let list t key =
  let open Lwt.Infix in
  find_blob t key >>= function
  | None -> Lwt.return (Error (`Not_found key))
  | Some tree -> begin
    match Git_store.read_exn t.store tree with
    | Tree t ->
      let r =
        List.filter_map
          (fun {Git_store.Tree.perm; name; _} ->
             let path = Mirage_kv.Key.add key name in
             match perm with
             | `Commit | `Dir -> Some (path, `Dictionary)
             | `Everybody | `Exec | `Normal -> Some (path, `Value)
             | `Link -> None)
          (Git_store.Tree.to_list t)
      in
      Lwt.return (Ok r)
    | _ -> Lwt.return (Error (`Dictionary_expected key))
  end

let last_modified t key =
  match t.committed, t.head with
  | None, None -> Lwt.return (Error (`Not_found key))
  | Some _, _ ->
    Lwt.return_ok
      (Option.fold ~none:Ptime.epoch ~some:Fun.id
         (Ptime.of_float_s (Int64.to_float (now ()))))
  | None, Some head ->
    (* See https://github.com/ocaml/ocaml/issues/9301 why we have the
       intermediate [r] value. *)
    let r = Git_store.read_exn t.store head in
    let[@warning "-8"] (Git_store.Object.Commit c) = r in
    let author = Git_store.Commit.author c in
    let secs, tz_offset = author.Git_store.User.date in
    let secs =
      Option.fold ~none:secs
        ~some:(fun {Git_store.User.sign; hours; minutes} ->
          let tz_off =
            Int64.(mul (add (mul (of_int hours) 60L) (of_int minutes)) 60L)
          in
          match sign with
          | `Plus -> Int64.(sub secs tz_off)
          | `Minus -> Int64.(add secs tz_off))
        tz_offset
    in
    let ts =
      Option.fold ~none:Ptime.epoch ~some:Fun.id
        (Ptime.of_float_s (Int64.to_float secs))
    in
    Lwt.return_ok ts

let digest t key =
  let open Lwt.Infix in
  find_blob t key
  >>= Option.fold
        ~none:(Lwt.return (Error (`Not_found key)))
        ~some:(fun x -> Lwt.return (Ok (SHA1.to_raw_string x)))

let size t key =
  let open Lwt_result.Infix in
  get t key >|= fun data -> Optint.Int63.of_int (String.length data)

let author ?(name = "Git KV") ?(email = "git-noreply@robur.coop") now =
  {Git_store.User.name; email; date= now (), None}

let rec unroll_tree t ~tree_root_hash (pred_perm, pred_name, pred_hash) rpath =
  let open Lwt.Infix in
  let ( >>? ) = Lwt_result.bind in
  match rpath with
  | [] -> begin
    match Git_store.read_exn t.store tree_root_hash with
    | Git_store.Object.Tree tree ->
      let tree =
        let open Git_store.Tree in
        add
          (entry ~name:pred_name pred_perm pred_hash)
          (remove ~name:pred_name tree)
      in
      let res = Git_store.write t.store (Git_store.Object.Tree tree) in
      begin
        match res with
        | Ok hash -> Lwt.return_ok hash
        | Error _ as err -> Lwt.return err
      end
    | _ -> assert false
  end
  | name :: rest -> begin
    Git_search.find t.store tree_root_hash (`Path (List.rev rpath)) >>= function
    | None ->
      let tree =
        Git_store.Tree.(v [entry ~name:pred_name pred_perm pred_hash])
      in
      Git_store.write t.store (Git_store.Object.Tree tree) |> Lwt.return
      >>? fun hash -> unroll_tree t ~tree_root_hash (`Dir, name, hash) rest
    | Some tree_hash -> begin
      match Git_store.read_exn t.store tree_hash with
      | Git_store.Object.Tree tree ->
        let tree =
          let open Git_store.Tree in
          add
            (entry ~name:pred_name pred_perm pred_hash)
            (remove ~name:pred_name tree)
        in
        Git_store.write t.store (Git_store.Object.Tree tree) |> Lwt.return
        >>? fun hash -> unroll_tree t ~tree_root_hash (`Dir, name, hash) rest
      | _ -> assert false
    end
  end

let tree_root_hash_of_store t =
  match t.committed, t.head with
  | Some tree_root_hash, _ -> Lwt.return_ok tree_root_hash
  | None, None ->
    let open Lwt_result.Infix in
    let tree = Git_store.Tree.v [] in
    Git_store.write t.store (Git_store.Object.Tree tree) |> Lwt.return
    >>= fun hash -> Lwt.return_ok hash
  | None, Some commit -> begin
    match Git_store.read_exn t.store commit with
    | Git_store.Object.Commit commit ->
      Lwt.return_ok (Git_store.Commit.tree commit)
    | _ ->
      Lwt.return_error
        (`Msg
           (Fmt.str "The current HEAD value (%a) is not a commit"
              Digestif.SHA1.pp commit))
  end

let ( >>? ) = Lwt_result.bind

let set ?and_commit t key contents =
  let segs = Mirage_kv.Key.segments key in
  match segs with
  | [] -> assert false (* TODO *)
  | path -> begin
    let blob = Git_store.Blob.of_string contents in
    let rpath = List.rev path in
    let name = List.hd rpath in
    let open Lwt_result.Infix in
    Git_store.write t.store (Git_store.Object.Blob blob) |> Lwt.return
    >>= fun hash ->
    tree_root_hash_of_store t >>= fun tree_root_hash ->
    unroll_tree t ~tree_root_hash (`Normal, name, hash) (List.tl rpath)
    >>= fun tree_root_hash ->
    match and_commit with
    | Some _old_tree_root_hash ->
      t.committed <- Some tree_root_hash;
      Lwt.return_ok ()
    | None ->
      let committer = author now in
      let author = author now in
      let action =
        Option.fold ~none:(`Create t.branch)
          ~some:(fun _ -> `Update (t.branch, t.branch))
          t.head
      in
      let parents = Option.to_list t.head in
      let commit =
        Git_store.Commit.make ~tree:tree_root_hash ~author ~committer ~parents
          (Some "Committed by git-kv")
      in
      Git_store.write t.store (Git_store.Object.Commit commit) |> Lwt.return
      >>= fun hash ->
      Git_store.Ref.write t.store t.branch (Git_store.Reference.uid hash)
      >>= fun () ->
      Lwt.Infix.(
        Git_sync.push ~capabilities ~ctx:t.ctx t.edn t.store [action]
        >|= Result.map_error (fun err ->
                `Msg
                  (Fmt.str "error pushing branch %a: %a" Git_store.Reference.pp
                     t.branch Git_sync.pp_error err))
        >>? fun () -> Git_store.shallow t.store hash >|= Result.ok)
      >>= fun () ->
      t.head <- Some hash;
      Lwt.return_ok ()
  end

let to_write_error (error : Git_store.error) =
  match error with
  | `Not_found hash -> `Hash_not_found hash
  | `Reference_not_found ref -> `Reference_not_found ref
  | `Msg err -> `Msg err

let set t key contents =
  let open Lwt.Infix in
  set ?and_commit:t.committed t key contents >|= Result.map_error to_write_error

let set_partial t key ~offset chunk =
  let open Lwt_result.Infix in
  get t key >>= fun contents ->
  let len = String.length contents in
  let add = String.length chunk in
  let off = Optint.Int63.to_int offset in
  if off < 0 then Lwt_result.fail (`Msg "offset does not fit into integer")
  else
    let res = Bytes.make (max len (off + add)) '\000' in
    Bytes.blit_string contents 0 res 0 len;
    Bytes.blit_string chunk 0 res off add;
    set t key (Bytes.unsafe_to_string res)

let remove ?and_commit t key =
  let segs = Mirage_kv.Key.segments key in
  match List.rev segs with
  | [] -> assert false
  | name :: [] -> (
    let open Lwt_result.Infix in
    tree_root_hash_of_store t >>= fun tree_root_hash ->
    let tree_root = Git_store.read_exn t.store tree_root_hash in
    let[@warning "-8"] (Git_store.Object.Tree tree_root) = tree_root in
    let tree_root = Git_store.Tree.remove ~name tree_root in
    let open Lwt_result.Infix in
    Git_store.write t.store (Git_store.Object.Tree tree_root) |> Lwt.return
    >>= fun tree_root_hash ->
    match and_commit with
    | Some _old_tree_root_hash ->
      t.committed <- Some tree_root_hash;
      Lwt.return_ok ()
    | None ->
      let committer = author now in
      let author = author now in
      let parents = Option.to_list t.head in
      let commit =
        Git_store.Commit.make ~tree:tree_root_hash ~author ~committer ~parents
          (Some "Committed by git-kv")
      in
      Git_store.write t.store (Git_store.Object.Commit commit) |> Lwt.return
      >>= fun hash ->
      Git_store.Ref.write t.store t.branch (Git_store.Reference.uid hash)
      >>= fun () ->
      Lwt.Infix.(
        Git_sync.push ~capabilities ~ctx:t.ctx t.edn t.store
          [`Update (t.branch, t.branch)]
        >|= Result.map_error (fun err ->
                `Msg
                  (Fmt.str "error pushing branch %a: %a" Git_store.Reference.pp
                     t.branch Git_sync.pp_error err))
        >>? fun () -> Git_store.shallow t.store hash >|= Result.ok)
      >>= fun () ->
      t.head <- Some hash;
      Lwt.return_ok ())
  | name :: pred_name :: rest -> (
    let open Lwt_result.Infix in
    tree_root_hash_of_store t >>= fun tree_root_hash ->
    let ( let* ) = Lwt.bind in
    let* res =
      Git_search.find t.store tree_root_hash
        (`Path (List.rev (pred_name :: rest)))
    in
    match res with
    | None -> Lwt.return_ok ()
    | Some hash -> begin
      match Git_store.read_exn t.store hash with
      | Git_store.Object.Tree tree -> (
        let tree = Git_store.Tree.remove ~name tree in
        Git_store.write t.store (Git_store.Object.Tree tree) |> Lwt.return
        >>= fun pred_hash ->
        unroll_tree t ~tree_root_hash (`Dir, pred_name, pred_hash) rest
        >>= fun tree_root_hash ->
        match and_commit with
        | Some _old_tree_root_hash ->
          t.committed <- Some tree_root_hash;
          Lwt.return_ok ()
        | None ->
          let committer = author now in
          let author = author now in
          let parents = Option.to_list t.head in
          let commit =
            Git_store.Commit.make ~tree:tree_root_hash ~author ~committer
              ~parents (Some "Committed by git-kv")
          in
          Git_store.write t.store (Git_store.Object.Commit commit) |> Lwt.return
          >>= fun hash ->
          Git_store.Ref.write t.store t.branch (Git_store.Reference.uid hash)
          >>= fun () ->
          (* TODO(dinosaure): better way to wrap monads! *)
          Lwt.Infix.(
            Git_sync.push ~capabilities ~ctx:t.ctx t.edn t.store
              [`Update (t.branch, t.branch)]
            >|= Result.map_error (fun err ->
                    `Msg
                      (Fmt.str "error pushing branch %a: %a"
                         Git_store.Reference.pp t.branch Git_sync.pp_error err))
            >>? fun () -> Git_store.shallow t.store hash >|= Result.ok)
          >>= fun () ->
          t.head <- Some hash;
          Lwt.return_ok ())
      | _ -> Lwt.return_ok ()
    end)

let remove t key =
  let open Lwt.Infix in
  remove ?and_commit:t.committed t key >|= Result.map_error to_write_error

let allocate t key ?last_modified:_ size =
  let open Lwt.Infix in
  exists t key >>= function
  | Error _ as e -> Lwt.return e
  | Ok (Some _) -> Lwt_result.fail (`Already_present key)
  | Ok None ->
    let size = Optint.Int63.to_int size in
    if size < 0 then Lwt_result.fail (`Msg "size does not fit into integer")
    else
      let data = String.make size '\000' in
      set t key data

let change_and_push
    t ?author:name ?author_email:email ?(message = "Committed by git-kv") f =
  let open Lwt.Infix in
  match t.committed with
  | Some _ -> Lwt.return_error (`Msg "Nested change_and_push")
  | None ->
    Lwt_mutex.with_lock t.mutex (fun () ->
        (let open Lwt_result.Infix in
         tree_root_hash_of_store t >>= fun tree_root_hash ->
         let t' = {t with committed= Some tree_root_hash} in
         let ( let* ) = Lwt.bind in
         let* res = f t' in
         (* XXX(dinosaure): we assume that only [change_and_push] can reset [t.committed] to [None] and
              we ensured that [change_and_push] can not be called into [f]. So we are sure that [t'.committed]
              must be [Some _] in anyway. *)
         let[@warning "-8"] (Some new_tree_root_hash) = t'.committed in
         if Digestif.SHA1.equal new_tree_root_hash tree_root_hash then
           Lwt.return_ok res (* XXX(dinosaure): nothing to send! *)
         else if not (Option.equal Digestif.SHA1.equal t.head t'.head) then
           Lwt.return
             (Error
                (`Msg
                   "store was modified outside of change_and_push, please retry"))
         else
           let action =
             Option.fold ~none:(`Create t.branch)
               ~some:(fun _ -> `Update (t.branch, t.branch))
               t.head
           in
           let parents = Option.to_list t.head in
           let author = author ?name ?email now in
           let committer = author in
           let commit =
             Git_store.Commit.make ~tree:new_tree_root_hash ~author ~committer
               ~parents (Some message)
           in
           Git_store.write t.store (Git_store.Object.Commit commit)
           |> Lwt.return
           >>= fun hash ->
           Git_store.Ref.write t.store t.branch (Git_store.Reference.uid hash)
           >>= fun () ->
           Log.debug (fun m -> m "Start to push changes!");
           (* TODO(dinosaure): better way to compose? *)
           Lwt.Infix.(
             Git_sync.push ~capabilities ~ctx:t.ctx t.edn t.store [action]
             >|= Result.map_error (fun err ->
                     `Msg
                       (Fmt.str "error pushing branch %a: %a"
                          Git_store.Reference.pp t.branch Git_sync.pp_error err))
             >>? fun () -> Git_store.shallow t.store hash >|= Result.ok)
           >>= fun () ->
           t.head <- Some hash;
           Lwt.return_ok res)
        >|= Result.map_error (fun err ->
                `Msg (Fmt.str "error pushing %a" Git_store.pp_error err)))

let rename t ~source ~dest =
  let open Lwt_result.Infix in
  let op t =
    get t source >>= fun contents ->
    remove t source >>= fun () -> set t dest contents
  in
  (* (hannes) we check whether we're in a change_and_push or not, since
       nested change_and_push are not supported. *)
  match t.committed with
  | Some _ -> op t
  | None -> begin
    let ( let* ) = Lwt.bind in
    let* res = change_and_push t op in
    match res with Ok a -> Lwt.return a | Error _ as e -> Lwt.return e
  end
