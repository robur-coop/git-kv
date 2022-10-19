(* The idea is to provide a Mirage_kv.RW interface that is backed by a git
   repository. The git repository is always (manually) kept in sync with the
   remote one: either this is the only writer (and thus only set/remove
   operations need to be pushed, or the API client receives a callback that
   some update was done, and proceeds with a pull. *)
type t

val connect : Mimic.ctx -> string -> t Lwt.t
(** [connect ctx remote] creates a new Git store which synchronizes
    with [remote] {i via} protocols available into the given [ctx].

    @raise [Invalid_argument _] if we can not initialize the store, or if
    we can not fetch the given [remote]. *)

val to_octets : t -> string Lwt.t
(** [to_octets store] returns a serialized version of the given [store]. *)

val of_octets : Mimic.ctx -> remote:string -> string ->
  (t, [> `Msg of string]) result Lwt.t
(** [of_octets ctx ~remote contents] tries to re-create a {!type:t} from its
    serialized version [contents]. This function does not do I/O and the
    returned {!type:t} can be out of sync with the given [remote]. We advise
    to call {!val:pull} to be in-sync with [remote]. *)

type change = [ `Add of Mirage_kv.Key.t
              | `Remove of Mirage_kv.Key.t
              | `Change of Mirage_kv.Key.t ]

val pull : t -> (change list, [> `Msg of string ]) result Lwt.t
(** [pull store] tries to synchronize the remote Git repository with your local
    [store] Git repository. It returns a list of changes between the old state
    of your store and what you have remotely. *)

val push : t -> (unit, [> `Msg of string ]) result Lwt.t
(** [push store] tries to push any changes from your local Git repository
    [store] to the remoe Git repository. The [push] function can fails for many
    reasons. Currently, we don't handle merge politics and how we can resolve
    conflicts between local and remote Git repositories. That mostly means that
    if you are the only one who push to the Git repository (into a specific
    branch), everything should be fine. But, if someone else push into the same
    remote Git repository, your change can be discarded by the remote server
    (due to conflicts). *)

module Make (Pclock : Mirage_clock.PCLOCK) : sig
  include Mirage_kv.RW
    with type t = t
     and type write_error = [ `Msg of string
                            | `Hash_not_found of Digestif.SHA1.t
                            | `Reference_not_found of Git.Reference.t
                            | Mirage_kv.write_error ]
  
  val size : t -> key -> (int, error) result Lwt.t
end
