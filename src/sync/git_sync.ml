let src = Logs.Src.create "git.sync"
let ( <.> ) f g = fun x -> f (g x)

module Log = (val Logs.src_log src : Logs.LOG)
module SHA1 = Digestif.SHA1

type error = [ `Exn of exn | `Git_store of Git_store.error | Mimic.error ]

open Lwt.Infix

let pp_error ppf = function
  | #Mimic.error as err -> Mimic.pp_error ppf err
  | `Exn exn -> Fmt.pf ppf "Exception: %s" (Printexc.to_string exn)
  | `Git_store err -> Fmt.pf ppf "Git_store error: %a" Git_store.pp_error err
  | `Invalid_flow -> Fmt.pf ppf "Invalid flow"

(*
let lightly_load t hash =
  Git_store.read_exn t hash >>= fun v ->
  let kind =
    match v with
    | Value.Commit _ -> `A
    | Value.Tree _ -> `B
    | Value.Blob _ -> `C
    | Value.Tag _ -> `D
  in
  let length = Int64.to_int (Git_store.Value.length v) in
  Lwt.return (kind, length)

let heavily_load t hash =
  Git_store.read_inflated t hash >>= function
  | Some (kind, { Cstruct.buffer; off; len }) ->
      let kind =
        match kind with
        | `Commit -> `A
        | `Tree -> `B
        | `Blob -> `C
        | `Tag -> `D
      in
      let raw = Bigstringaf.copy buffer ~off ~len in
      let value = Carton.Value.make ~kind raw in
      Lwt.return value 
  | None -> raise Not_found
*)

let ( >>? ) x f =
  x >>= function Ok x -> f x | Error err -> Lwt.return_error err

let fetch
    ?(push_stdout = ignore)
    ?(push_stderr = ignore)
    ?threads
    ~ctx
    endpoint
    t
    ?version
    ?capabilities
    ?deepen
    want =
  let want, src_dst_mapping =
    match want with
    | (`All | `None) as x -> x, fun src -> [src]
    | `Some src_dst_refs ->
      let src_refs = List.map fst src_dst_refs in
      let src_dst_map =
        List.fold_left
          (fun src_dst_map (src_ref, dst_ref) ->
            try
              let dst_refs = Git_store.Reference.Map.find src_ref src_dst_map in
              if List.exists (Git_store.Reference.equal dst_ref) dst_refs then
                src_dst_map
              else
                Git_store.Reference.Map.add src_ref (dst_ref :: dst_refs)
                  src_dst_map
            with Not_found ->
              Git_store.Reference.Map.add src_ref [dst_ref] src_dst_map)
          Git_store.Reference.Map.empty src_dst_refs
      in
      let src_dst_mapping src_ref =
        Git_store.Reference.Map.find_opt src_ref src_dst_map
        |> Option.value ~default:[src_ref]
      in
      `Some src_refs, src_dst_mapping
  in
  Log.debug (fun m -> m "Start to fetch the PACK file.");
  Smart_git.fetch ~push_stdout ~push_stderr ?threads ~ctx t endpoint ?version
    ?capabilities ?deepen want
  >>? function
  | `Empty ->
    Log.debug (fun m -> m "No PACK file was transmitted");
    Lwt.return_ok None
  | `Pack (uid, refs) ->
    let update (src_ref, hash) =
      let write_dst_ref dst_ref =
        Git_store.Ref.write t dst_ref (Git_store.Reference.Uid hash)
        >>= function
        | Ok v -> Lwt.return v
        | Error err ->
          Log.warn (fun m ->
              m "Impossible to update %a to %a: %a." Git_store.Reference.pp
                src_ref SHA1.pp hash Git_store.pp_error err);
          Lwt.return_unit
      in
      let dst_refs = src_dst_mapping src_ref in
      Lwt_list.iter_p write_dst_ref dst_refs
    in
    Lwt_list.iter_p update refs >>= fun () -> Lwt.return_ok (Some (uid, refs))

let push ~ctx endpoint t ?version ?capabilities cmds =
  Smart_git.push ~ctx (t, Hashtbl.create 0) endpoint ?version ?capabilities cmds
