let () =
  let git_daemon = Bos.Cmd.(v "git" % "daemon" % "--help") in

  match Bos.OS.Cmd.run_status ~quiet:true git_daemon with
  | Ok (`Exited 0) -> print_endline "(true)"
  | _ -> print_endline "false"
