let src = Logs.Src.create "git-fetch"

module SHA1 = Digestif.SHA1
module Log = (val Logs.src_log src : Logs.LOG)
module Pate = Carton_git_lwt.Make (SHA1)

let ( >>? ) = Lwt_result.bind
let ( <.> ) f g = fun x -> f (g x)

let explode store ?threads stream =
  let open Lwt.Infix in
  let buf = Bbuffer.create 0x7ff in
  let append str ~off ~len =
    Log.debug (fun m ->
        m "PACK: @[<hov>%a@]"
          (Hxd_string.pp Hxd.default)
          (String.sub str off len));
    Bbuffer.add_substring buf str off len;
    Lwt.return_unit
  in
  let map buf ~pos:off len =
    let len = Int.min len (Bbuffer.length buf - off) in
    Bbuffer.sub buf off len
  in
  let cachet = Cachet.make ~pagesize:4096 ~map buf in
  let cfg = Pate.config ?threads () in
  Log.debug (fun m -> m "Start to verify the incoming PACK file (and save it)");
  Pate.verify_from_stream ~cfg ~append cachet stream >>= fun (entries, hash) ->
  Log.debug (fun m -> m "Hash of the PACK file: %s" (Ohex.encode hash));
  let offsets = Hashtbl.create 0x7ff in
  let fn = function
    | Carton.Resolved_base {cursor; uid; _} | Resolved_node {cursor; uid; _} ->
      Hashtbl.add offsets uid cursor
    | _ -> ()
  in
  Array.iter fn entries;
  let index (uid : Carton.Uid.t) =
    match Hashtbl.find_opt offsets uid with
    | Some cursor -> Carton.Local cursor
    | None ->
      let hash = SHA1.of_raw_string (uid :> string) in
      let obj = Git_store.read_exn store hash in
      let kind =
        match obj with
        | Git_store.Object.Commit _ -> `A
        | Git_store.Object.Tree _ -> `B
        | Git_store.Object.Blob _ -> `C
        | Git_store.Object.Tag _ -> `D
      in
      let bstr = Git_store.Object.to_bstr obj in
      Carton.Extern (kind, bstr)
  in
  let t = Pate.make ~index cachet in
  let fn = function
    | Carton.Resolved_base {cursor; _}
    | Resolved_node {cursor; _}
    | Unresolved_base {cursor; _}
    | Unresolved_node {cursor} -> (
      let size = Carton.size_of_offset t ~cursor Carton.Size.zero in
      let blob = Carton.Blob.make ~size in
      let value = Carton.of_offset t blob ~cursor in
      let kind =
        match Carton.Value.kind value with
        | `A -> `Commit
        | `B -> `Tree
        | `C -> `Blob
        | `D -> `Tag
      in
      let len = Carton.Value.length value in
      let bstr = Carton.Value.bigstring value in
      let bstr = Bstr.sub bstr ~off:0 ~len in
      match Git_store.Object.of_bstr ~kind bstr with
      | Ok value ->
        let _ = Git_store.write store value in
        ()
      | Error _ -> ())
  in
  Array.iter fn entries; Lwt.return_ok hash

let fetch_v1
    ?(uses_git_transport = false)
    ~push_stdout
    ~push_stderr
    ~capabilities
    path
    flow
    ?deepen
    ?want
    host
    store
    fetch_cfg
    push =
  let open Lwt.Infix in
  Lwt.try_bind
    (fun () ->
      Fetch.fetch_v1 ~uses_git_transport ~push_stdout ~push_stderr ~capabilities
        ?deepen ?want ~host path
        (Smart_flow.Flow.make flow)
        (store, Hashtbl.create 0x7ff)
        fetch_cfg
      @@ fun (payload, off, len) ->
      let v = String.sub payload off len in
      push (Some (v, 0, len)))
    (fun refs ->
      Log.debug (fun m -> m "Got new reference(s) from the server");
      push None >>= fun () ->
      Mimic.close flow >>= fun () -> Lwt.return_ok refs)
  @@ fun exn ->
  push None >>= fun () ->
  Mimic.close flow >>= fun () -> Lwt.reraise exn

let default_capabilities =
  [`Side_band_64k; `Multi_ack_detailed; `Ofs_delta; `Thin_pack; `Report_status]

type transmission = [ `Git | `Exec ]

let rec get_transmission :
    Mimic.edn list ->
    [ `Git | `Exec | `HTTP of Uri.t * Git_store.Endpoint.handshake ] option =
  function
  | Mimic.Edn (k, v) :: r -> (
    match Mimic.equal k Git_store.Endpoint.git_transmission with
    | Some Mimic.Refl -> Some v
    | None -> get_transmission r)
  | [] -> None

let add_unless lst k v =
  match List.assoc_opt (String.lowercase_ascii k) lst with
  | Some _ -> lst
  | None -> (String.lowercase_ascii k, v) :: lst

let pp_version ppf = function
  | `V1 -> Fmt.pf ppf "1"
  | _ -> Fmt.pf ppf "unknown"

let add_headers_for_fetching ?(version = `V1) ctx =
  let headers =
    Option.value ~default:[] (Mimic.get Git_store.Endpoint.git_http_headers ctx)
  in
  let headers =
    add_unless headers "content-type" "application/x-git-upload-pack-request"
  in
  let headers =
    add_unless headers "accept" "application/x-git-upload-pack-result"
  in
  let headers =
    add_unless headers "git-protocol" (Fmt.str "version=%a" pp_version version)
  in
  Mimic.replace Git_store.Endpoint.git_http_headers headers ctx

let fetch
    ?(push_stdout = ignore)
    ?(push_stderr = ignore)
    ?(bounds = 10)
    ?threads
    ~ctx
    store
    edn
    ?(version = `V1)
    ?(capabilities = default_capabilities)
    ?deepen
    want =
  let open Lwt.Infix in
  let hostname = edn.Git_store.Endpoint.hostname in
  let path = edn.Git_store.Endpoint.path in
  let stream, emitter = Lwt_stream.create_bounded bounds in
  let pusher_with_logging = function
    | Some (str, off, len) ->
      Log.debug (fun m -> m "Download %d byte(s) of the PACK file." len);
      emitter#push (String.sub str off len)
    | None ->
      Log.debug (fun m -> m "End of pack.");
      emitter#close;
      Lwt.return_unit
  in
  let ctx =
    Mimic.add Git_store.Endpoint.git_capabilities `Rd
      (Git_store.Endpoint.to_ctx edn ctx)
  in
  let ctx = add_headers_for_fetching ~version ctx in
  Lwt.catch (fun () ->
      Mimic.unfold ctx >>? fun ress ->
      Mimic.connect ress >>= fun flow ->
      match flow, get_transmission ress, version with
      | Ok flow, Some (#transmission as transmission), `V1 -> (
        let fetch_cfg = Fetch.configuration capabilities in
        let uses_git_transport =
          match transmission with `Git -> true | `Exec -> false
        in
        Log.debug (fun m -> m "Start to negotiate and unpack");
        Lwt.both
          (fetch_v1 ~push_stdout ~push_stderr ~uses_git_transport ~capabilities
             path flow ?deepen ~want hostname store fetch_cfg
             pusher_with_logging)
          (explode ?threads store stream)
        >>= fun (refs, idx) ->
        match refs, idx with
        | Ok [], _ -> Lwt.return_ok `Empty
        | Ok refs, Ok uid -> Lwt.return_ok (`Pack (uid, refs))
        | (Error _ as err), _ -> Lwt.return err
        | Ok _refs, (Error _ as err) -> Lwt.return err)
      | Ok flow, Some (`HTTP (uri, handshake)), `V1 -> (
        let fetch_cfg = Fetch.configuration ~stateless:true capabilities in
        let uri0 = Fmt.str "%a/info/refs?service=git-upload-pack" Uri.pp uri in
        let uri0 = Uri.of_string uri0 in
        let uri1 = Fmt.str "%a/git-upload-pack" Uri.pp uri in
        let uri1 = Uri.of_string uri1 in
        Lwt.both
          ( handshake ~uri0 ~uri1 flow >>= fun () ->
            fetch_v1 ~push_stdout ~push_stderr ~capabilities path flow ?deepen
              ~want hostname store fetch_cfg pusher_with_logging )
          (explode ?threads store stream)
        >>= fun (refs, idx) ->
        match refs, idx with
        | Ok refs, Ok uid -> Lwt.return_ok (`Pack (uid, refs))
        | (Error _ as err), _ -> Lwt.return err
        | Ok [], _ -> Lwt.return_ok `Empty
        | Ok _refs, (Error _ as err) -> Lwt.return err)
      | Ok flow, Some _, _ ->
        Log.err (fun m -> m "The protocol version is uninmplemented.");
        Mimic.close flow >>= fun () ->
        Lwt.return_error (`Msg "Version protocol unimplemented")
      | Ok flow, None, _ ->
        Log.err (fun m ->
            m "A flow was allocated but we can not recognize the transmission.");
        Mimic.close flow >>= fun () ->
        Lwt.return_error (`Msg "Unrecognized protocol")
      | Error err, _, _ ->
        Log.err (fun m -> m "The Git peer is not reachable.");
        Lwt.return_error err)
  @@ function
  | Failure err -> Lwt.return_error (`Msg err)
  | exn -> Lwt.return_error (`Exn exn)

let pack store uids =
  let load (uid : Carton.Uid.t) _ =
    let hash = SHA1.of_raw_string (uid :> string) in
    let value = Git_store.read_exn store hash in
    let kind =
      match value with
      | Git_store.Object.Commit _ -> `A
      | Git_store.Object.Tree _ -> `B
      | Git_store.Object.Blob _ -> `C
      | Git_store.Object.Tag _ -> `D
    in
    let bstr = Git_store.Object.to_bstr value in
    Lwt.return (Carton.Value.make ~kind bstr)
  in
  let with_header = List.length uids in
  let uids = Lwt_seq.of_list uids in
  let fn hash =
    let uid = Carton.Uid.unsafe_of_string (SHA1.to_raw_string hash) in
    let value = Git_store.read_exn store hash in
    let kind =
      match value with
      | Git_store.Object.Commit _ -> `A
      | Git_store.Object.Tree _ -> `B
      | Git_store.Object.Blob _ -> `C
      | Git_store.Object.Tag _ -> `D
    in
    let length = Int64.to_int (Git_store.Object.length value) in
    Cartonnage.Entry.make ~kind ~length uid ()
  in
  let entries = Lwt_seq.map fn uids in
  let targets = Pate.delta ~load entries in
  let seq =
    Pate.to_pack ~with_header ~with_signature:SHA1.empty ~load
      (Lwt_stream.of_lwt_seq targets)
  in
  let stream, push = Lwt_stream.create () in
  let open Lwt.Infix in
  Lwt.async (fun () ->
      Lwt_seq.iter (push <.> Option.some) seq >|= fun () -> push None);
  fun () -> Lwt_stream.get stream

let push_v1
    ?uses_git_transport
    flow
    ~capabilities
    path
    cmds
    hostname
    store
    push_cfg
    pack =
  let open Lwt.Infix in
  Push.push ?uses_git_transport ~capabilities cmds ~host:hostname path
    (Smart_flow.Flow.make flow)
    store push_cfg pack
  >>= fun () ->
  Mimic.close flow >>= fun () -> Lwt.return_ok ()

let add_headers_for_pushing ?(version = `V1) ctx =
  let open Git_store.Endpoint in
  let headers = Option.value ~default:[] (Mimic.get git_http_headers ctx) in
  let headers =
    add_unless headers "content-type" "application/x-git-receive-pack-request"
  in
  let headers =
    add_unless headers "accept" "application/x-git-receive-pack-result"
  in
  let headers =
    add_unless headers "git-protocol" (Fmt.str "version=%a" pp_version version)
  in
  Mimic.replace git_http_headers headers ctx

let push
    ~ctx
    ((store, _) as t)
    edn
    ?(version = `V1)
    ?(capabilities = default_capabilities)
    cmds =
  let open Lwt.Infix in
  let hostname = edn.Git_store.Endpoint.hostname in
  let path = edn.Git_store.Endpoint.path in
  let ctx =
    Mimic.add Git_store.Endpoint.git_capabilities `Wr
      (Git_store.Endpoint.to_ctx edn ctx)
  in
  let ctx = add_headers_for_pushing ~version ctx in
  Lwt.catch (fun () ->
      Mimic.unfold ctx >>? fun ress ->
      Mimic.connect ress >>= fun res ->
      match res, get_transmission ress, version with
      | Ok flow, Some (#transmission as transmission), `V1 ->
        let push_cfg = Push.configuration () in
        let uses_git_transport =
          match transmission with `Git -> true | `Exec -> false
        in
        push_v1 ~uses_git_transport flow ~capabilities path cmds hostname t
          push_cfg (pack store)
      | Ok flow, Some (`HTTP (uri, handshake)), `V1 ->
        let push_cfg = Push.configuration ~stateless:true () in
        let uri0 =
          Fmt.str "%a/info/refs?service=git-receive-pack" Uri.pp uri
          |> Uri.of_string
        in
        let uri1 = Fmt.str "%a/git-receive-pack" Uri.pp uri |> Uri.of_string in
        handshake ~uri0 ~uri1 flow >>= fun () ->
        push_v1 flow ~capabilities path cmds hostname t push_cfg (pack store)
      | Ok flow, Some _, _ ->
        Log.err (fun m -> m "The protocol version is uninmplemented.");
        Mimic.close flow >>= fun () ->
        Lwt.return_error (`Msg "Version protocol unimplemented")
      | Ok flow, None, _ ->
        Log.err (fun m ->
            m "A flow was allocated but we can not recognize the transmission.");
        Mimic.close flow >>= fun () ->
        Lwt.return_error (`Msg "Unrecognized protocol")
      | Error err, _, _ ->
        Log.err (fun m -> m "The Git peer is not reachable.");
        Lwt.return_error err)
  @@ function
  | Failure err -> Lwt.return_error (`Msg err)
  | exn -> Lwt.return_error (`Exn exn)
