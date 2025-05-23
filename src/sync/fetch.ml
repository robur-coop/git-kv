type configuration = Neg.configuration

let multi_ack capabilities =
  match
    ( List.exists (( = ) `Multi_ack) capabilities,
      List.exists (( = ) `Multi_ack_detailed) capabilities )
  with
  | true, true | false, true -> `Detailed
  | true, false -> `Some
  | false, false -> `None

let no_done = List.exists (( = ) `No_done)

let configuration ?(stateless = false) capabilities =
  {
    Neg.stateless;
    Neg.no_done= (if stateless then true else no_done capabilities);
    Neg.multi_ack= multi_ack capabilities;
  }

let src = Logs.Src.create "git-sync.fetch"

module Log = (val Logs.src_log src : Logs.LOG)
module SHA1 = Digestif.SHA1
open Lwt.Infix

let is_a_tag ref =
  List.exists (String.equal "tags") (Git_store.Reference.segs ref)

let references want have =
  match want with
  | `None -> [], []
  | `All ->
    List.fold_left
      (fun acc -> function
        | uid, ref, false when not (is_a_tag ref) -> (uid, ref) :: acc
        | _ -> acc)
      [] have
    |> List.split
  | `Some refs ->
    let fold acc (uid, value, peeled) =
      if List.exists Git_store.Reference.(equal value) refs && not peeled then
        (uid, value) :: acc
      else acc
    in
    List.fold_left fold [] have |> List.split

let fetch_v1
    ?(uses_git_transport = false)
    ?(push_stdout = ignore)
    ?(push_stderr = ignore)
    ~capabilities
    ?deepen
    ?want:(refs = `None)
    ~host
    path
    flow
    store
    fetch_cfg
    pack =
  let my_caps =
    (* XXX(dinosaure): HTTP ([stateless]) enforces no-done capabilities. Otherwise, you never
       will receive the PACK file. *)
    if fetch_cfg.Neg.no_done && not (no_done capabilities) then
      `No_done :: capabilities
    else capabilities
  in
  let prelude ctx =
    let open Smart in
    let* () =
      if uses_git_transport then
        send ctx proto_request (Proto_request.upload_pack ~host ~version:1 path)
      else return ()
    in
    let* v = recv ctx advertised_refs in
    let v =
      Smart.Advertised_refs.map ~fuid:SHA1.of_hex ~fref:Git_store.Reference.v v
    in
    let uids, refs = references refs (Smart.Advertised_refs.refs v) in
    Smart.Context.replace_their_caps ctx (Smart.Advertised_refs.capabilities v);
    return (uids, refs)
  in
  let ctx = Smart.Context.make ~my_caps in
  let negotiator = Neg.make ~compare:SHA1.unsafe_compare in
  Neg.tips store negotiator >>= fun () ->
  Smart_flow.run flow (prelude ctx) >>= fun (uids, refs) ->
  Neg.find_common flow fetch_cfg store negotiator ctx ?deepen uids >>= function
  | `Close ->
    Log.debug (fun m -> m "Close the negotiation");
    Lwt.return []
  | `Continue res ->
    let recv_pack ctx =
      let open Smart in
      let side_band =
        Smart.Context.is_cap_shared ctx `Side_band
        || Smart.Context.is_cap_shared ctx `Side_band_64k
      in
      recv ctx (recv_pack ~push_stdout ~push_stderr side_band)
    in
    if res < 0 then Logs.warn (fun m -> m "No common commits");
    let rec go () =
      Smart_flow.run flow (recv_pack ctx) >>= function
      | `End_of_transmission -> Lwt.return ()
      | `Payload (str, off, len) -> pack (str, off, len) >>= go
      | `Stdout -> go ()
      | `Stderr -> go ()
    in
    go () >>= fun () -> Lwt.return (List.combine refs uids)
