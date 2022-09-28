(* The idea is to provide a Mirage_kv.RW interface that is backed by a git
   repository. The git repository is always (manually) kept in sync with the
   remote one: either this is the only writer (and thus only set/remove
   operations need to be pushed, or the API client receives a callback that
   some update was done, and proceeds with a pull. *)

include Mirage_kv.RW
  with type write_error = [ `Msg of string
                          | `Hash_not_found of Digestif.SHA1.t
                          | `Reference_not_found of Git.Reference.t
                          | Mirage_kv.write_error ]

val connect : Mimic.ctx -> string -> t Lwt.t

val to_octets : t -> string Lwt.t

val of_octets : Mimic.ctx -> remote:string -> string ->
  (t, [> `Msg of string]) result Lwt.t

type change = [ `Add of key
              | `Remove of key
              | `Change of key ]

val pull : t -> (change list, [> `Msg of string ]) result Lwt.t
val size : t -> key -> (int, error) result Lwt.t
