let ( let* ) = Result.bind

let run_it cmd =
  let* status = Bos.OS.Cmd.run_status cmd in
  if status = `Exited 0 then Ok ()
  else
    Error
      (`Msg ("status not 0, but " ^ Fmt.to_to_string Bos.OS.Cmd.pp_status status))

let empty_repo () =
  let* cwd = Bos.OS.Dir.current () in
  let* tmpdir = Bos.OS.Dir.tmp ~dir:cwd "git-kv-%s" in
  let* () = Bos.OS.Dir.set_current tmpdir in
  let cmd = Bos.Cmd.(v "git" % "init" % "--bare" % "-q") in
  let* () = run_it cmd in
  let* () = Bos.OS.Dir.set_current cwd in
  let cmd =
    Bos.Cmd.(
      v "git"
      % "daemon"
      % "--base-path=."
      % "--export-all"
      % "--reuseaddr"
      % "--pid-file=pid"
      % "--detach"
      % "--port=9419"
      % "--enable=receive-pack") in
  let* () = run_it cmd in
  let pid = Result.get_ok (Bos.OS.File.read (Fpath.v "pid")) in
  Ok (Fpath.basename tmpdir, String.trim pid)

let kill_git pid =
  try Unix.kill (int_of_string pid) Sys.sigterm
  with Unix.Unix_error _ -> Printf.eprintf "Error killing git\n"

open Lwt.Infix

let read_in_change_and_push () =
  match
    let* tmpdir, pid = empty_repo () in
    Fun.protect
      ~finally:(fun () -> kill_git pid)
      (fun () ->
        Lwt_main.run
          ( Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
            Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
            Git_kv.change_and_push t (fun t ->
                Git_kv.set t (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
                if Result.is_error r then Alcotest.fail "failure writing"
                ; Git_kv.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
                  if Result.is_error r then Alcotest.fail "failure reading"
                  ; Alcotest.(check string)
                      "Git_kv.get" "value" (Result.get_ok r)
                  ; Lwt.return_unit)
            >>= fun r ->
            if Result.is_error r then Alcotest.fail "failure change_and_push"
            ; Git_kv.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
              if Result.is_error r then Alcotest.fail "failure reading outside"
              ; Alcotest.(check string) "Git_kv.get" "value" (Result.get_ok r)
              ; Lwt.return_unit ))
    ; Ok ()
  with
  | Ok () -> ()
  | Error (`Msg msg) -> print_endline ("got an error from bos: " ^ msg)

let set_outside_change_and_push () =
  match
    let* tmpdir, pid = empty_repo () in
    Fun.protect
      ~finally:(fun () -> kill_git pid)
      (fun () ->
        Lwt_main.run
          ( Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
            Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
            Git_kv.change_and_push t (fun t' ->
                Git_kv.set t' (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
                if Result.is_error r then Alcotest.fail "failure writing foo"
                ; Git_kv.set t (Mirage_kv.Key.v "/bar") "other value"
                  >>= fun r ->
                  if Result.is_error r then Alcotest.fail "failure writing bar"
                  ; Lwt.return_unit)
            >>= fun r ->
            if Result.is_ok r then
              Alcotest.fail "expected change_and_push failure"
            ; Git_kv.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
              if Result.is_ok r then
                Alcotest.fail "expected failure reading foo"
              ; Git_kv.get t (Mirage_kv.Key.v "/bar") >>= fun r ->
                if Result.is_error r then
                  Alcotest.fail "failure reading outside bar"
                ; Alcotest.(check string)
                    "Git_kv.get bar" "other value" (Result.get_ok r)
                ; Lwt.return_unit ))
    ; Ok ()
  with
  | Ok () -> ()
  | Error (`Msg msg) -> print_endline ("got an error from bos: " ^ msg)

let remove_in_change_and_push () =
  match
    let* tmpdir, pid = empty_repo () in
    Fun.protect
      ~finally:(fun () -> kill_git pid)
      (fun () ->
        Lwt_main.run
          ( Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
            Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
            Git_kv.set t (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
            if Result.is_error r then Alcotest.fail "failure writing"
            ; Git_kv.change_and_push t (fun t' ->
                  Git_kv.get t' (Mirage_kv.Key.v "/foo") >>= fun r ->
                  if Result.is_error r then
                    Alcotest.fail "failure reading inside"
                  ; Alcotest.(check string)
                      "Git_kv.get" "value" (Result.get_ok r)
                  ; Git_kv.remove t' (Mirage_kv.Key.v "/foo") >>= fun r ->
                    if Result.is_error r then Alcotest.fail "failure removing"
                    ; Git_kv.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
                      if Result.is_error r then
                        Alcotest.fail "failure reading the outer t"
                      ; Alcotest.(check string)
                          "Git_kv.get" "value" (Result.get_ok r)
                      ; Lwt.return_unit)
              >>= fun r ->
              if Result.is_error r then Alcotest.fail "failure change_and_push"
              ; Git_kv.get t (Mirage_kv.Key.v "/foo") >>= fun r ->
                if Result.is_ok r then
                  Alcotest.fail "expected failure reading outside"
                ; Lwt.return_unit ))
    ; Ok ()
  with
  | Ok () -> ()
  | Error (`Msg msg) -> print_endline ("got an error from bos: " ^ msg)

let last_modified_in_change_and_push () =
  match
    let* tmpdir, pid = empty_repo () in
    Fun.protect
      ~finally:(fun () -> kill_git pid)
      (fun () ->
        Lwt_main.run
          ( Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
            Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
            Git_kv.set t (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
            if Result.is_error r then Alcotest.fail "failure writing"
            ; Git_kv.last_modified t (Mirage_kv.Key.v "/foo") >>= fun r ->
              if Result.is_error r then Alcotest.fail "failure last_modified"
              ; let lm = Result.get_ok r in
                Git_kv.change_and_push t (fun t' ->
                    Git_kv.last_modified t' (Mirage_kv.Key.v "/foo")
                    >>= fun r ->
                    if Result.is_error r then
                      Alcotest.fail "failure last_modified inside"
                    ; let lm' = Result.get_ok r in
                      Alcotest.(check bool)
                        "last modified is later or equal" true
                        (Ptime.is_later ~than:lm lm' || Ptime.equal lm lm')
                      ; Git_kv.set t' (Mirage_kv.Key.v "/foo") "new value"
                        >>= fun r ->
                        if Result.is_error r then
                          Alcotest.fail "failure writing inside"
                        ; Git_kv.last_modified t' (Mirage_kv.Key.v "/foo")
                          >>= fun r ->
                          if Result.is_error r then
                            Alcotest.fail
                              "failure last_modified inside after set"
                          ; let lm2 = Result.get_ok r in
                            Alcotest.(check bool)
                              "last modified is later" true
                              (Ptime.is_later ~than:lm' lm2
                              || Ptime.equal lm' lm2)
                            ; Lwt.return lm2)
                >>= fun r ->
                if Result.is_error r then
                  Alcotest.fail "failure change_and_push"
                ; let lm2 = Result.get_ok r in
                  Git_kv.last_modified t (Mirage_kv.Key.v "/foo") >>= fun r ->
                  if Result.is_error r then
                    Alcotest.fail "failure last_modified after change_and_push"
                  ; let lm3 = Result.get_ok r in
                    Alcotest.(check bool)
                      "last modified is later" true
                      (Ptime.is_later ~than:lm lm3 || Ptime.equal lm lm3)
                    ; Alcotest.(check bool)
                        "last modified is later outside than inside" true
                        (Ptime.is_later ~than:lm2 lm3 || Ptime.equal lm2 lm3)
                    ; Lwt.return_unit ))
    ; Ok ()
  with
  | Ok () -> ()
  | Error (`Msg msg) -> print_endline ("got an error from bos: " ^ msg)

let digest_in_change_and_push () =
  match
    let* tmpdir, pid = empty_repo () in
    Fun.protect
      ~finally:(fun () -> kill_git pid)
      (fun () ->
        Lwt_main.run
          ( Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
            Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
            Git_kv.set t (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
            if Result.is_error r then Alcotest.fail "failure writing"
            ; Git_kv.digest t (Mirage_kv.Key.v "/foo") >>= fun r ->
              if Result.is_error r then Alcotest.fail "failure digest"
              ; let digest = Result.get_ok r in
                Git_kv.change_and_push t (fun t' ->
                    Git_kv.digest t' (Mirage_kv.Key.v "/foo") >>= fun r ->
                    if Result.is_error r then
                      Alcotest.fail "failure digest inside"
                    ; Alcotest.(check string)
                        "Git_kv.digest" digest (Result.get_ok r)
                    ; Git_kv.set t' (Mirage_kv.Key.v "/foo") "something else"
                      >>= fun r ->
                      if Result.is_error r then Alcotest.fail "failure set"
                      ; Git_kv.digest t' (Mirage_kv.Key.v "/foo") >>= fun r ->
                        if Result.is_error r then
                          Alcotest.fail "failure digest inside"
                        ; Alcotest.(check bool)
                            "Git_kv.digest" false
                            (String.equal digest (Result.get_ok r))
                        ; Lwt.return_unit)
                >>= fun r ->
                if Result.is_error r then
                  Alcotest.fail "failure change_and_push"
                ; Git_kv.digest t (Mirage_kv.Key.v "/foo") >>= fun r ->
                  if Result.is_error r then
                    Alcotest.fail "failure digest outside"
                  ; Alcotest.(check bool)
                      "Git_kv.digest" false
                      (String.equal digest (Result.get_ok r))
                  ; Lwt.return_unit ))
    ; Ok ()
  with
  | Ok () -> ()
  | Error (`Msg msg) -> print_endline ("got an error from bos: " ^ msg)

let multiple_change_and_push () =
  match
    let* tmpdir, pid = empty_repo () in
    Fun.protect
      ~finally:(fun () -> kill_git pid)
      (fun () ->
        Lwt_main.run
          ( Git_unix.ctx (Happy_eyeballs_lwt.create ()) >>= fun ctx ->
            Git_kv.connect ctx ("git://localhost:9419/" ^ tmpdir) >>= fun t ->
            let wait = Lwt_mvar.create_empty () in
            let task_c () =
              Git_kv.change_and_push t (fun t''' ->
                  print_endline "running 3"
                  ; print_endline "running 3 - now get"
                  ; Git_kv.get t''' (Mirage_kv.Key.v "/foo") >>= fun r ->
                    if Result.is_error r then
                      failwith "failure reading foo in third change_and_push"
                    ; assert (String.equal "value 2" (Result.get_ok r))
                    ; print_endline "running 3 - now set"
                    ; Git_kv.set t''' (Mirage_kv.Key.v "/foo") "value 3"
                      >>= fun r ->
                      if Result.is_error r then
                        failwith "failure writing foo in third change_and_push"
                      ; print_endline "running 3 - now get again"
                      ; Git_kv.get t''' (Mirage_kv.Key.v "/foo") >>= fun r ->
                        if Result.is_error r then
                          failwith
                            "failure reading foo in third change_and_push \
                             adter the write"
                        ; assert (String.equal "value 3" (Result.get_ok r))
                        ; print_endline "running 3 - now finished"
                        ; Lwt.return_unit)
              >>= fun r ->
              if Result.is_error r then
                failwith "failure second change_and_push"
              ; Lwt_mvar.put wait () in
            let task_b () =
              Git_kv.change_and_push t (fun t'' ->
                  print_endline "running 2"
                  ; Lwt.async task_c
                  ; print_endline "running 2 - now get"
                  ; Git_kv.get t'' (Mirage_kv.Key.v "/foo") >>= fun r ->
                    if Result.is_error r then
                      failwith "failure reading foo in second change_and_push"
                    ; assert (String.equal "value" (Result.get_ok r))
                    ; print_endline "running 2 - now set"
                    ; Git_kv.set t'' (Mirage_kv.Key.v "/foo") "value 2"
                      >>= fun r ->
                      if Result.is_error r then
                        failwith "failure writing foo in second change_and_push"
                      ; print_endline "running 2 - now get again"
                      ; Git_kv.get t'' (Mirage_kv.Key.v "/foo") >>= fun r ->
                        if Result.is_error r then
                          failwith
                            "failure reading foo in second change_and_push \
                             adter the write"
                        ; assert (String.equal "value 2" (Result.get_ok r))
                        ; print_endline "running 2 - finished"
                        ; Lwt.return_unit)
              >|= fun r ->
              if Result.is_error r then
                failwith "failure second change_and_push" in
            let task_a () =
              Git_kv.change_and_push t (fun t' ->
                  print_endline "running 1"
                  ; Lwt.async task_b
                  ; print_endline "running 1 - now set"
                  ; Git_kv.set t' (Mirage_kv.Key.v "/foo") "value" >>= fun r ->
                    if Result.is_error r then
                      Alcotest.fail
                        "failure writing foo in first change_and_push"
                    ; print_endline "running 1 - now get"
                    ; Git_kv.get t' (Mirage_kv.Key.v "/foo") >>= fun r ->
                      if Result.is_error r then
                        Alcotest.fail
                          "failure reading foo in first change_and_push, after \
                           the write"
                      ; Alcotest.(check string)
                          "Git_kv.get foo" "value" (Result.get_ok r)
                      ; print_endline "running 1 - finished"
                      ; Lwt.return_unit)
              >|= fun r ->
              if Result.is_error r then
                Alcotest.fail "failure first change_and_push" in
            task_a () >>= fun () ->
            Lwt_mvar.take wait >>= fun () ->
            Git_kv.get t (Mirage_kv.Key.v "/foo") >|= fun r ->
            if Result.is_error r then
              Alcotest.fail "failure reading outside foo"
            ; Alcotest.(check string)
                "Git_kv.get bar" "value 3" (Result.get_ok r) ))
    ; Ok ()
  with
  | Ok () -> ()
  | Error (`Msg msg) -> print_endline ("got an error from bos: " ^ msg)

let basic_tests =
  [
    "Read in change_and_push", `Quick, read_in_change_and_push
  ; "Set outside change_and_push", `Quick, set_outside_change_and_push
  ; "Remove in change_and_push", `Quick, remove_in_change_and_push
  ; "Last modified in change_and_push", `Quick, last_modified_in_change_and_push
  ; "Digest in change_and_push", `Quick, digest_in_change_and_push
  ; "Multiple change_and_push", `Quick, multiple_change_and_push
  ]

let tests = ["Basic tests", basic_tests]
let () = Alcotest.run "Git-KV alcotest tests" tests
