let src = Logs.Src.create "git.sync.mstore"
let ( <.> ) f g = fun x -> f (g x)

module Log = (val Logs.src_log src : Logs.LOG)
module SHA1 = Digestif.SHA1
open Lwt.Infix

let get_commit_for_negotiation (t, mem) hash =
  Log.debug (fun m -> m "Load commit %a." SHA1.pp hash);
  match Hashtbl.find mem hash with
  | v -> Lwt.return_some v
  | exception Not_found -> begin
    (* XXX(dinosaure): given hash can not exist into [t],
     * in this call we try to see if remote hashes are available
     * locally. *)
    match Git_store.read t hash with
    | Ok (Git_store.Object.Commit commit) ->
      let {Git_store.User.date= ts, _; _} = Git_store.Commit.committer commit in
      let v = hash, ref 0, ts in
      Hashtbl.add mem hash v; Lwt.return_some v
    | Ok _ | Error _ -> Lwt.return_none
  end

let get = get_commit_for_negotiation

let parents_of_commit t hash =
  Log.debug (fun m -> m "Get parents of %a." SHA1.pp hash);
  match Git_store.read_exn t hash with
  | Git_store.Object.Commit commit -> begin
    Git_store.is_shallowed t hash >>= function
    | false -> Lwt.return (Git_store.Commit.parents commit)
    | true -> Lwt.return []
  end
  | _ -> Lwt.return []

let parents ((t, _mem) as store) hash =
  parents_of_commit t hash >>= fun parents ->
  let fold acc hash =
    get_commit_for_negotiation store hash >>= function
    | Some v -> Lwt.return (v :: acc)
    | None -> Lwt.return acc
  in
  Lwt_list.fold_left_s fold [] parents

let deref (t, _) refname =
  Log.debug (fun m -> m "Dereference %a." Git_store.Reference.pp refname);
  Git_store.Ref.resolve t refname >>= function
  | Ok hash -> Lwt.return_some hash
  | Error _ -> Lwt.return_none

let locals (t, _) =
  Log.debug (fun m -> m "Load locals references.");
  Git_store.Ref.list t >>= Lwt_list.map_p (Lwt.return <.> fst)

let shallowed (t, _) =
  Log.debug (fun m -> m "Shallowed commits of the store.");
  Git_store.shallowed t

let shallow (t, _) hash =
  Log.debug (fun m -> m "Shallow %a." SHA1.pp hash);
  Git_store.shallow t hash

let unshallow (t, _) hash =
  Log.debug (fun m -> m "Unshallow %a." SHA1.pp hash);
  Git_store.unshallow t hash
