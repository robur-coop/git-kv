let () = Printexc.record_backtrace true

let reporter ppf =
  let report src level ~over k msgf =
    let k _ = over (); k () in
    let with_metadata header _tags k ppf fmt =
      Format.kfprintf k ppf
        ("[%a]%a[%a]: " ^^ fmt ^^ "\n%!")
        Fmt.(styled `Blue int)
        (Unix.getpid ()) Logs_fmt.pp_header (level, header)
        Fmt.(styled `Magenta string)
        (Logs.Src.name src)
    in
    msgf @@ fun ?header ?tags fmt -> with_metadata header tags k ppf fmt
  in
  {Logs.report}

(*
let () = Fmt_tty.setup_std_outputs ~style_renderer:`Ansi_tty ~utf_8:true ()
let () = Logs.set_reporter (reporter Fmt.stderr)
let () = Logs.set_level ~all:true (Some Logs.Debug)
*)

open Lwt.Infix

let get ~quiet store key =
  Git_kv.get store key >>= function
  | Ok contents when not quiet ->
    Fmt.pr "@[<hov>%a@]\n%!" (Hxd_string.pp Hxd.default) contents;
    Lwt.return (Ok 0)
  | Ok _ -> Lwt.return (Ok 0)
  | Error err ->
    if not quiet then Fmt.epr "%a.\n%!" Git_kv.pp_error err;
    Lwt.return (Ok 1)

let exists ~quiet store key =
  Git_kv.exists store key >>= function
  | Ok k when not quiet ->
    (match k with
    | None -> Fmt.pr "%a does not exists\n%!" Mirage_kv.Key.pp key
    | Some `Dictionary ->
      Fmt.pr "%a exists as a dictionary\n%!" Mirage_kv.Key.pp key
    | Some `Value -> Fmt.pr "%a exists as a value\n%!" Mirage_kv.Key.pp key);
    Lwt.return (Ok 0)
  | Ok _ -> Lwt.return (Ok 0)
  | Error err ->
    if not quiet then Fmt.epr "%a.\n%!" Git_kv.pp_error err;
    Lwt.return (Ok 1)

let value_of_string str =
  let v = ref None in
  match Scanf.sscanf str "%S" (fun str -> v := Some str) with
  | () -> Option.get !v
  | exception _ ->
    Scanf.sscanf str "%s" (fun str -> v := Some str);
    Option.get !v

let set ~quiet store key str =
  let value = value_of_string str in
  Git_kv.set store key value >>= function
  | Ok () -> Lwt.return (Ok 0)
  | Error err ->
    if not quiet then Fmt.epr "%a.\n%!" Git_kv.pp_write_error err;
    Lwt.return (Ok 1)

let remove ~quiet store key =
  Git_kv.remove store key >>= function
  | Ok () -> Lwt.return (Ok 0)
  | Error err ->
    if not quiet then Fmt.epr "%a.\n%!" Git_kv.pp_write_error err;
    Lwt.return (Ok 1)

let list ~quiet store key =
  Git_kv.list store key >>= function
  | Ok lst when not quiet ->
    List.iter
      (fun (name, k) ->
        match k with
        | `Dictionary -> Fmt.pr "d %a\n%!" Mirage_kv.Key.pp name
        | `Value -> Fmt.pr "- %a\n%!" Mirage_kv.Key.pp name)
      lst;
    Lwt.return (Ok 0)
  | Ok _ -> Lwt.return (Ok 0)
  | Error err ->
    if not quiet then Fmt.epr "%a.\n%!" Git_kv.pp_error err;
    Lwt.return (Ok 1)

let last_modified ~quiet store key =
  Git_kv.last_modified store key >>= function
  | Ok time ->
    Fmt.pr "%a\n%!" Ptime.pp time;
    Lwt.return (Ok 0)
  | Error err ->
    if not quiet then Fmt.epr "%a.\n%!" Git_kv.pp_error err;
    Lwt.return (Ok 1)

let pull ~quiet store =
  Git_kv.pull store >>= function
  | Error (`Msg err) ->
    if not quiet then Fmt.epr "%s.\n%!" err;
    Lwt.return (Ok 1)
  | Ok diff when not quiet ->
    List.iter
      (function
        | `Add key -> Fmt.pr "+ %a\n%!" Mirage_kv.Key.pp key
        | `Remove key -> Fmt.pr "- %a\n%!" Mirage_kv.Key.pp key
        | `Change key -> Fmt.pr "* %a\n%!" Mirage_kv.Key.pp key)
      diff;
    Lwt.return (Ok 0)
  | Ok _ -> Lwt.return (Ok 0)

let save store filename =
  let oc = open_out filename in
  let stream = Git_kv.to_octets store in
  Lwt_stream.iter_p (fun str -> output_string oc str; Lwt.return_unit) stream
  >>= fun () -> close_out oc; Lwt.return (Ok 0)

let trim lst =
  List.fold_left (fun acc -> function "" -> acc | str -> str :: acc) [] lst
  |> List.rev

let with_key ~f key =
  match Mirage_kv.Key.v key with
  | key -> f key
  | exception _ ->
    Fmt.epr "Invalid key: %S.\n%!" key;
    Lwt.return (Ok 1)

let repl store fd_in =
  let is_a_tty = Unix.isatty fd_in in
  let ic = Unix.in_channel_of_descr fd_in in
  let rec go store0 =
    if is_a_tty then Fmt.pr "# %!";
    match String.split_on_char ' ' (input_line ic) |> trim with
    | ["get"; key] ->
      with_key ~f:(get ~quiet:false store0) key >|= ignore >>= fun () ->
      go store0
    | ["exists"; key] ->
      with_key ~f:(exists ~quiet:false store0) key >|= ignore >>= fun () ->
      go store0
    | "set" :: key :: data ->
      let data = String.concat " " data in
      with_key ~f:(fun key -> set ~quiet:false store0 key data) key >|= ignore
      >>= fun () -> go store0
    | ["remove"; key] ->
      with_key ~f:(remove ~quiet:false store0) key >|= ignore >>= fun () ->
      go store0
    | ["list"; key] ->
      with_key ~f:(list ~quiet:false store0) key >|= ignore >>= fun () ->
      go store0
    | ["mtime"; key] ->
      with_key ~f:(last_modified ~quiet:false store0) key >|= ignore
      >>= fun () -> go store0
    | ["pull"] ->
      if is_a_tty then Fmt.pr "\n%!";
      pull ~quiet:false store0 >|= ignore >>= fun () -> go store0
    | ["quit"] -> Lwt.return ()
    | ["fold"] ->
      Git_kv.change_and_push store0 (fun store1 -> go store1)
      >|= Result.fold ~ok:Fun.id ~error:(function `Msg msg ->
              Fmt.epr "%s.\n%!" msg)
      >>= fun () -> go store0
    | ["save"; filename] ->
      save store0 filename >|= ignore >>= fun _ ->
      if is_a_tty then Fmt.pr "\n%!";
      go store0
    | _ ->
      Fmt.epr "Invalid command.\n%!";
      go store0
    | exception End_of_file -> Lwt.return ()
  in
  go store

let run remote = function
  | None ->
    Lwt_main.run
    @@ ( Git_net.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
         Git_kv.connect ctx remote >>= fun t -> repl t Unix.stdin )
  | Some filename ->
    let contents =
      let ic = open_in filename in
      let ln = in_channel_length ic in
      let bs = Bytes.create ln in
      really_input ic bs 0 ln; Bytes.unsafe_to_string bs
    in
    Lwt_main.run
      ( Git_net.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
        let stream = Lwt_stream.of_list [contents] in
        Git_kv.of_octets ctx ~remote stream >>= function
        | Ok t -> repl t Unix.stdin
        | Error (`Msg err) -> Fmt.failwith "%s." err )

let run remote filename_opt =
  let () = Mirage_crypto_rng_unix.use_default () in
  run remote filename_opt

let () =
  match Sys.argv with
  | [|_; remote|] -> run remote None
  | [|_; remote; filename|] when Sys.file_exists filename ->
    run remote (Some filename)
  | _ -> Fmt.epr "%s <remote> [<filename>]\n%!" Sys.argv.(0)
