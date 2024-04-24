# v0.0.4 2024-04-24 - Paris (France)

- Add a branch accessor (@dinosaure, #30)
- Add the compression level argument into `to_octets` (@hannesm, @dinosaure, #31)
- Add few `Lwt.pause` to be cooperative with processes (@hannesm, @dinosaure, #32)

# v0.0.3 2022-12-16 - Berlin (Germany)

- Allow author, author_email, and message being specified in `change_and_push`
  (#28 @hannes)

# v0.0.2 2022-12-14 - Berlin (Germany)

- Use Stdlib.Result and Fmt instead of Rresult (#24 @hannes)
- Use Git.Reference.main (available since git 3.10.0, #24 @hannes)
- change_and_push may return errors, report them (#24 @hannes)
- delete unix transitive dependencies from tests (#25 @dinosaure)
- update to mirage-kv 6.0.0 (#27 @hannes)

# v0.0.1 2022-11-02 - Paris (France)

- First release
