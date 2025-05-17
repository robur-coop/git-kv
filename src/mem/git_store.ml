module SHA1 = Digestif.SHA1
module Reference = Git_reference
module Commit = Git_commit
module Tree = Git_tree
module Blob = Git_blob
module Tag = Git_tag
module Object = Git_object
module User = Git_user
module Endpoint = Git_endpoint

let src = Logs.Src.create "git.store"

module Log = (val Logs.src_log src : Logs.LOG)

type t = {
  values: (SHA1.t, Git_object.t) Hashtbl.t;
  refs: (Git_reference.t, [ `H of SHA1.t | `R of Git_reference.t ]) Hashtbl.t;
  shallows: Git_shallow.t;
  root: Fpath.t;
  mutable head: Git_reference.contents option;
}

let read_exn t h = Hashtbl.find t.values h
let is_shallowed t hash = Git_shallow.exists t.shallows ~equal:SHA1.equal hash
let shallowed t = Git_shallow.get t.shallows
let shallow t hash = Git_shallow.append t.shallows hash
let unshallow t hash = Git_shallow.remove t.shallows ~equal:SHA1.equal hash
let read t h = try Ok (read_exn t h) with _ -> Error (`Not_found h)

let write t value =
  let hash = Git_object.digest value in
  Hashtbl.replace t.values hash value;
  Ok hash

let v root =
  {
    values= Hashtbl.create 0x7ff;
    refs= Hashtbl.create 0x7ff;
    shallows= Git_shallow.make [];
    root;
    head= None;
  }
  |> Lwt.return_ok

module Traverse = Traverse_bfs.Make (struct
  type nonrec t = t

  let root {root; _} = root
  let read_exn = read_exn
  let is_shallowed = is_shallowed
end)

let fold = Traverse.fold
let iter = Traverse.iter

module Ref = struct
  module Graph = Git_reference.Map

  let list t =
    Log.debug (fun l -> l "Ref.list.");
    let graph, rest =
      Hashtbl.fold
        (fun k -> function
          | `R ptr -> fun (a, r) -> a, (k, ptr) :: r
          | `H hash -> fun (a, r) -> Graph.add k hash a, r)
        t.refs (Graph.empty, [])
    in
    let graph =
      List.fold_left
        (fun a (k, ptr) ->
          try
            let v = Graph.find ptr a in
            Graph.add k v a
          with Not_found -> a)
        graph rest
    in
    let r = Graph.fold (fun k v a -> (k, v) :: a) graph [] in
    Lwt.return r

  let mem t r =
    Log.debug (fun l -> l "Ref.mem %a." Git_reference.pp r);
    try
      let _ = Hashtbl.find t.refs r in
      Lwt.return true
    with Not_found -> Lwt.return false

  exception Cycle

  let resolve t r =
    let rec go ~visited r =
      Log.debug (fun l -> l "Ref.resolve %a." Git_reference.pp r);
      try
        if List.exists (Git_reference.equal r) visited then raise Cycle;
        match Hashtbl.find t.refs r with
        | `H s ->
          Log.debug (fun l ->
              l "Ref.resolve %a found: %a." Git_reference.pp r SHA1.pp s);
          Lwt.return_ok s
        | `R r' ->
          let visited = r :: visited in
          go ~visited r'
      with
      | Not_found ->
        Log.err (fun l -> l "%a not found." Git_reference.pp r);
        Lwt.return_error (`Reference_not_found r)
      | Cycle ->
        Log.err (fun l -> l "Got a reference cycle");
        Lwt.return_error `Cycle
    in
    go ~visited:[] r

  let read t r =
    try
      match Hashtbl.find t.refs r with
      | `H hash -> Lwt.return_ok (Git_reference.uid hash)
      | `R refname -> Lwt.return_ok (Git_reference.ref refname)
    with Not_found -> Lwt.return_error (`Reference_not_found r)

  let remove t r =
    Log.debug (fun l -> l "Ref.remove %a." Git_reference.pp r);
    Hashtbl.remove t.refs r;
    Lwt.return_ok ()

  let write t r value =
    Log.debug (fun l -> l "Ref.write %a." Git_reference.pp r);
    let head_contents =
      match value with
      | Git_reference.Uid hash -> `H hash
      | Ref refname -> `R refname
    in
    Hashtbl.replace t.refs r head_contents;
    Lwt.return_ok ()
end

type error =
  [ `Not_found of SHA1.t
  | `Reference_not_found of Git_reference.t
  | `Msg of string ]

let pp_error ppf = function
  | `Not_found hash -> Fmt.pf ppf "%a not found" SHA1.pp hash
  | `Reference_not_found ref ->
    Fmt.pf ppf "Reference %a not found" Git_reference.pp ref
  | `Msg str -> Fmt.string ppf str
