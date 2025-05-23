let src = Logs.Src.create "pck"

module Log = (val Logs.src_log src : Logs.LOG)
module SHA1 = Digestif.SHA1

type color = Black | White
type none = Leaf
type commit = {root: SHA1.t; preds: SHA1.t list}

type 'preds kind =
  | Commit : commit kind
  | Tree : SHA1.t list kind
  | Blob : none kind
  | Tag : SHA1.t kind

let commit = Commit
let tree = Tree
let blob = Blob
let tag = Tag

type t =
  | Node : {
      mutable color: color;
      ts: int64 option;
      uid: SHA1.t;
      kind: 'preds kind;
      preds: 'preds;
    }
      -> t

let make = fun ~kind preds ?ts uid -> Node {color= White; ts; uid; kind; preds}

let compare (Node a) (Node b) =
  match a.ts, b.ts with
  | Some a, Some b -> Int64.compare b a
  | _ -> SHA1.unsafe_compare a.uid b.uid

let preds =
 fun (Node {kind; preds; _}) ->
  match kind with
  | Commit -> preds.root :: preds.preds
  | Tree -> preds
  | Blob -> []
  | Tag -> [preds]

let memoize ~fn =
  let ( >>= ) = Lwt.bind in

  let tbl = Hashtbl.create 0x100 in
  ( tbl,
    fun key ->
      match Hashtbl.find tbl key with
      | value -> Lwt.return value
      | exception Not_found ->
        fn key >>= fun value ->
        Hashtbl.replace tbl key value;
        Lwt.return value )

let commands ~capabilities store cmds have =
  let ( >>= ) = Lwt.bind in
  let fold acc = function
    | `Create reference -> (
      Git_mstore.deref store reference >>= function
      | Some uid -> Lwt.return (Smart.Commands.create uid reference :: acc)
      | None -> Lwt.return acc)
    | `Delete reference -> (
      match
        List.find_opt
          (fun (_, reference', peeled) ->
            Git_store.Reference.equal reference reference' && peeled = false)
          have
      with
      | Some (uid, _, _) ->
        Lwt.return (Smart.Commands.delete uid reference :: acc)
      | None -> Lwt.return acc)
    | `Update (local, remote) -> (
      Git_mstore.deref store local >>= function
      | None -> Lwt.return acc
      | Some uid_new -> (
        match
          List.find_opt
            (fun (_, reference', peeled) ->
              Git_store.Reference.equal remote reference' && peeled = false)
            have
        with
        | Some (uid_old, _, _) ->
          Lwt.return (Smart.Commands.update uid_old uid_new remote :: acc)
        | None -> Lwt.return (Smart.Commands.create uid_new remote :: acc)))
  in
  let rec go a = function
    | [] -> Lwt.return a
    | head :: tail -> fold a head >>= fun a -> go a tail
  in
  go [] cmds >>= function
  | [] -> Lwt.return None
  | head :: tail ->
    Lwt.return (Some (Smart.Commands.v ~capabilities ~others:tail head))

let get_limits : type uid ref.
    compare:(uid -> uid -> int) ->
    (uid * ref * bool) list ->
    (uid, ref) Smart.Commands.command list ->
    uid list * uid list =
 fun ~compare:compare_uid have cmds ->
  let module Set = Set.Make (struct
    type t = uid

    let compare = compare_uid
  end) in
  let exclude =
    let fold acc (uid, _, _) = Set.add uid acc in
    List.fold_left fold Set.empty have
  in
  let exclude =
    let fold acc = function
      | Smart.Commands.Create _ -> acc
      | Smart.Commands.Delete (uid, _) -> Set.add uid acc
      | Smart.Commands.Update (uid, _, _) -> Set.add uid acc
    in
    List.fold_left fold exclude cmds
  in
  let sources =
    let fold acc = function
      | Smart.Commands.Update (_, uid, _) -> Set.add uid acc
      | Smart.Commands.Create (uid, _) -> Set.add uid acc
      | Smart.Commands.Delete _ -> acc
    in
    List.fold_left fold Set.empty cmds
  in
  Set.elements exclude, Set.elements sources

let get (store, _) hash =
  let open Lwt.Infix in
  match Git_store.read store hash with
  | Ok (Git_store.Object.Blob _) -> Lwt.return_some (make ~kind:blob Leaf hash)
  | Ok (Git_store.Object.Tree tree) ->
    let hashes = Git_store.Tree.hashes tree in
    Lwt.return_some (make ~kind:Tree hashes hash)
  | Ok (Git_store.Object.Commit commit) ->
    ( Git_store.is_shallowed store hash >|= function
      | true -> []
      | false -> Git_store.Commit.parents commit )
    >>= fun preds ->
    let root = Git_store.Commit.tree commit in
    let {Git_store.User.date= ts, _; _} = Git_store.Commit.committer commit in
    Lwt.return_some (make ~kind:Commit {root; preds} ~ts hash)
  | Ok (Git_store.Object.Tag tag) ->
    let pred = Git_store.Tag.obj tag in
    Lwt.return_some (make ~kind:Tag pred hash)
  | Error _ -> Lwt.return_none

let get_uncommon_objects =
 fun store ~exclude ~sources ->
  let ( >>= ) = Lwt.bind in
  let ( >>| ) x f = Lwt.map f x in
  let fold_left_s ~f a l =
    let rec go a = function
      | [] -> Lwt.return a
      | x :: r -> f a x >>= fun a -> go a r
    in
    go a l
  in
  let map_p ~fn l =
    let rec go = function
      | [] -> Lwt.return []
      | x :: r ->
        fn x >>= fun x ->
        go r >>= fun r -> Lwt.return (x :: r)
    in
    go l
  in

  let tbl, get = memoize ~fn:(get store) in

  let module K = struct
    type t = SHA1.t

    let compare = SHA1.unsafe_compare
  end in
  let module P = struct
    type nonrec t = t

    let compare = compare
  end in
  let module Psq = Psq.Make (K) (P) in
  let all_blacks psq =
    let fold _ (Node {color; _}) acc = color = Black && acc in
    Psq.fold fold true psq
  in

  let propagate (Node {color; _} as node) =
    let q = Queue.create () in
    let rec go () =
      match Queue.pop q with
      | uid -> begin
        match Hashtbl.find tbl uid with
        | Some (Node value as node) ->
          value.color <- color;
          List.iter (fun uid -> Queue.push uid q) (preds node);
          go ()
        | None | (exception Not_found) -> go ()
      end
      | exception Queue.Empty -> ()
    in
    List.iter (fun uid -> Queue.push uid q) (preds node);
    go ()
  in

  let propagate_snapshot (Node {color; _} as node) =
    let q = Queue.create () in
    let rec go () =
      match Queue.pop q with
      | uid ->
        let tip = function
          | Some (Node value as node) ->
            value.color <- color;
            List.iter (fun uid -> Queue.add uid q) (preds node)
          | None -> ()
        in
        get uid >>| tip >>= fun () -> go ()
      | exception Queue.Empty -> Lwt.return ()
    in
    List.iter (fun uid -> Queue.push uid q) (preds node);
    go ()
  in

  let rec garbage psq =
    if all_blacks psq then Lwt.return ()
    else
      match Psq.pop psq with
      | Some ((_, (Node {color= Black; _} as node)), psq) ->
        let fold psq uid =
          get uid >>= function
          | Some (Node ({kind= Tree; _} as value) as node) ->
            value.color <- Black;
            propagate_snapshot node >>= fun () -> Lwt.return psq
          | Some (Node ({color= White; _} as value) as node) ->
            value.color <- Black;
            propagate node;
            let psq = Psq.add uid node psq in
            Lwt.return psq
          | Some node ->
            let psq = Psq.add uid node psq in
            Lwt.return psq
          | None -> Lwt.return psq
        in
        fold_left_s ~f:fold psq (preds node) >>= garbage
      | Some ((_, node), psq) ->
        let fold psq uid =
          get uid >>= function
          | Some node -> Lwt.return (Psq.add uid node psq)
          | None -> Lwt.return psq
        in
        fold_left_s ~f:fold psq (preds node) >>= garbage
      | None -> Lwt.return ()
  in

  let map_sources uid =
    get uid >>= function
    | Some (Node {kind= Commit; preds; _} as node) ->
      let {root; _} = preds in
      ( get root >>= function
        | Some tree -> propagate_snapshot tree
        | None -> Lwt.return () )
      >>= fun () -> Lwt.return (Some (uid, node))
    | Some node -> Lwt.return (Some (uid, node))
    | None -> Lwt.return None
  in
  let map_exclude uid =
    get uid >>= function
    | Some (Node ({kind= Commit; preds; _} as value) as node) ->
      value.color <- Black;
      let {root; _} = preds in
      ( get root >>= function
        | Some (Node value as tree) ->
          value.color <- Black;
          propagate_snapshot tree
        | None -> Lwt.return () )
      >>= fun () -> Lwt.return (Some (uid, node))
    | Some (Node value as node) ->
      value.color <- Black;
      Lwt.return (Some (uid, node))
    | None -> Lwt.return None
  in
  map_p ~fn:map_sources sources >>= fun sources ->
  map_p ~fn:map_exclude exclude >>= fun exclude ->
  let fold acc = function
    | Some (key, value) -> Psq.add key value acc
    | None -> acc
  in
  let psq = List.fold_left fold Psq.empty (List.append exclude sources) in
  garbage psq >>= fun () ->
  let fold uid value acc =
    match value with Some (Node {color= White; _}) -> uid :: acc | _ -> acc
  in
  Lwt.return (Hashtbl.fold fold tbl [])
