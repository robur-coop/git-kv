let git_daemon_exists () =
  match Unix.create_process "git" [| "git"; "daemon" |] Unix.stdin Unix.stdout Unix.stderr with
  | pid ->
    Unix.kill pid Sys.sigint ; true
  | exception Unix.Unix_error _ -> false

let () =
  let v = git_daemon_exists () in
  Format.printf "%b" v
