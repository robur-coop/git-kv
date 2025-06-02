type configuration = {stateless: bool}

let configuration ?(stateless = false) () = {stateless}

open Lwt.Infix

let src = Logs.Src.create "push"

module Log = (val Logs.src_log src : Logs.LOG)
module SHA1 = Digestif.SHA1

let push
    ?(uses_git_transport = false)
    ~capabilities:my_caps
    cmds
    ~host
    path
    flow
    t
    {stateless}
    pack =
  let fiber ctx =
    let open Smart in
    let* () =
      if uses_git_transport then
        send ctx proto_request
          (Proto_request.receive_pack ~host ~version:1 path)
      else return ()
    in
    let* v = recv ctx advertised_refs in
    Context.replace_their_caps ctx (Smart.Advertised_refs.capabilities v);
    return
      (Smart.Advertised_refs.map ~fuid:SHA1.of_hex ~fref:Git_store.Reference.v v)
  in
  let ctx = Smart.Context.make ~my_caps in
  Smart_flow.run flow (fiber ctx) >>= fun advertised_refs ->
  Pack.commands ~capabilities:my_caps t cmds
    (Smart.Advertised_refs.refs advertised_refs)
  >>= function
  | None ->
    Smart_flow.run flow Smart.(send ctx flush ()) >>= fun () -> Lwt.return ()
  | Some cmds -> (
    Smart_flow.run flow
      Smart.(
        send ctx commands
          (Commands.map ~fuid:SHA1.to_hex ~fref:Git_store.Reference.to_string
             cmds))
    >>= fun () ->
    let exclude, sources =
      Pack.get_limits ~compare
        (Smart.Advertised_refs.refs advertised_refs)
        (Smart.Commands.commands cmds)
    in
    Pack.get_uncommon_objects t ~exclude ~sources >>= fun uids ->
    Log.debug (fun m -> m "Prepare a pack of %d object(s)." (List.length uids));
    let stream = pack uids in
    let side_band =
      Smart.Context.is_cap_shared ctx `Side_band
      || Smart.Context.is_cap_shared ctx `Side_band_64k
    in
    let pack = Smart.send_pack ~stateless side_band in
    let rec go () =
      stream () >>= function
      | None ->
        let report_status = Smart.Context.is_cap_shared ctx `Report_status in
        Log.debug (fun m -> m "report-status capability: %b." report_status);
        if report_status then
          Smart_flow.run flow Smart.(recv ctx (status side_band))
          >|= Smart.Status.map ~fn:Git_store.Reference.v
        else if uses_git_transport then
          Smart_flow.run flow Smart.(recv ctx recv_flush) >>= fun () ->
          let cmds = List.map Result.ok (Smart.Commands.commands cmds) in
          Lwt.return (Smart.Status.v cmds)
        else
          let cmds = List.map Result.ok (Smart.Commands.commands cmds) in
          Lwt.return (Smart.Status.v cmds)
      | Some payload ->
        Smart_flow.run flow Smart.(send ctx pack payload) >>= fun () -> go ()
    in
    go () >>= fun status ->
    match Smart.Status.to_result status with
    | Ok () ->
      Log.debug (fun m -> m "Push is done!");
      Log.info (fun m ->
          m "%a" Smart.Status.pp
            (Smart.Status.map ~fn:Git_store.Reference.to_string status));
      Lwt.return ()
    | Error err ->
      Log.err (fun m -> m "Push got an error: %s" err);
      Lwt.return ())
