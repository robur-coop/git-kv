(* The idea is to provide a Mirage_kv.RW interface that is backed by a git
   repository. The git repository is always (manually) kept in sync with the
   remote one: either this is the only writer (and thus only set/remove
   operations need to be pushed, or the API client receives a callback that
   some update was done, and proceeds with a pull. *)

include Mirage_kv.RO

val connect : Mimic.ctx -> string -> t Lwt.t

val to_octets : t -> string Lwt.t

val of_octets : string -> (t, [`Msg of string]) result Lwt.t

val pull : t -> (unit, [ `Msg of string ]) result Lwt.t
