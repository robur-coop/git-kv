let ( <.> ) f g x = f (g x)

type ('a, 'err) t =
  | Read of {
      buffer: bytes;
      off: int;
      len: int;
      k: int -> ('a, 'err) t;
      eof: unit -> ('a, 'err) t;
    }
  | Write of {buffer: string; off: int; len: int; k: int -> ('a, 'err) t}
  | Return of 'a
  | Error of 'err

module type CONTEXT = sig
  type t
  type encoder
  type decoder

  val pp : Capability.t Fmt.t -> t Fmt.t
  val encoder : t -> encoder
  val decoder : t -> decoder
end

module type VALUE = sig
  type 'a send
  type 'a recv
  type error
  type encoder
  type decoder

  val encode : encoder -> 'a send -> 'a -> (unit, error) t
  val decode : decoder -> 'a recv -> ('a, error) t
end

module Context = struct
  type capabilities = {
    my_caps: Capability.t list;
    their_caps: Capability.t list;
  }

  type t = {
    encoder: Encoder.encoder;
    decoder: Decoder.decoder;
    mutable capabilities: capabilities;
  }

  type encoder = Encoder.encoder
  type decoder = Decoder.decoder

  let pp _pp_ctx _ppf _t = ()

  let make ~my_caps =
    let capabilities = {my_caps; their_caps= []} in
    {encoder= Encoder.create (); decoder= Decoder.create (); capabilities}

  let with_decoder ~my_caps decoder =
    {
      encoder= Encoder.create ();
      decoder;
      capabilities= {my_caps; their_caps= []};
    }

  let encoder {encoder; _} = encoder
  let decoder {decoder; _} = decoder
  let capabilities {capabilities; _} = capabilities

  let replace_their_caps ctx their_caps =
    ctx.capabilities <- {ctx.capabilities with their_caps}

  let is_cap_shared {capabilities= {my_caps; their_caps}; _} cap =
    let is_cap_in caps = List.exists (fun c -> Capability.equal c cap) caps in
    is_cap_in my_caps && is_cap_in their_caps
end

module Scheduler
    (Context : CONTEXT)
    (Value :
      VALUE
        with type encoder = Context.encoder
         and type decoder = Context.decoder) =
struct
  type error = Value.error

  let bind : ('a, 'err) t -> f:('a -> ('b, 'err) t) -> ('b, 'err) t =
    let rec bind' m ~f =
      match m with
      | Return v -> f v
      | Error _ as err -> err
      | Read ({k; eof; _} as rd) ->
        Read {rd with k= bind' ~f <.> k; eof= bind' ~f <.> eof}
      | Write ({k; _} as wr) -> Write {wr with k= bind' ~f <.> k}
    in
    bind'

  let return v = Return v
  let fail error = Error error
  let map m ~f = bind m ~f:(fun v -> return (f v))
  let ( >>= ) m f = bind m ~f
  let ( >|= ) m f = map m ~f
  let ( let* ) m f = m >>= f
  let ( let+ ) m f = m >|= f

  let reword_error f x =
    let rec map_error = function
      | Return _ as r -> r
      | Error err -> Error (f err)
      | Read ({k; eof; _} as rd) ->
        Read {rd with k= map_error <.> k; eof= map_error <.> eof}
      | Write ({k; _} as wr) -> Write {wr with k= map_error <.> k}
    in
    map_error x

  (* Is slightly different from [m |> reword_error ~f >>= f1].
     The places where [apply] used currently the alternative code above would be sufficient,
     but that would end up in twice the number of function calls *)
  let apply m ~bind_ret ~bind_err =
    let rec apply' = function
      | Return r -> bind_ret r
      | Error err -> bind_err err
      | Read ({k; eof; _} as rd) ->
        Read {rd with k= apply' <.> k; eof= apply' <.> eof}
      | Write ({k; _} as wr) -> Write {wr with k= apply' <.> k}
    in
    apply' m

  let encode : type a.
      Context.t ->
      a Value.send ->
      a ->
      (Context.t -> ('b, [> `Protocol of error ]) t) ->
      ('b, [> `Protocol of error ]) t =
   fun ctx w v k ->
    let encoder = Context.encoder ctx in
    Value.encode encoder w v
    |> apply
         ~bind_ret:(fun () -> k ctx)
         ~bind_err:(fun err -> Error (`Protocol err))

  let send : type a.
      Context.t -> a Value.send -> a -> (unit, [> `Protocol of error ]) t =
   fun ctx w x -> encode ctx w x (fun _ctx -> Return ())

  let decode : type a.
      Context.t ->
      a Value.recv ->
      (Context.t -> a -> ('b, [> `Protocol of error ]) t) ->
      ('b, [> `Protocol of error ]) t =
   fun ctx w k ->
    let decoder = Context.decoder ctx in
    Value.decode decoder w
    |> apply
         ~bind_ret:(fun v -> k ctx v)
         ~bind_err:(fun e -> Error (`Protocol e))

  let recv : type a. Context.t -> a Value.recv -> (a, [> `Protocol of error ]) t
      =
   fun ctx w -> decode ctx w (fun _ctx v -> Return v)

  let error_msgf fmt = Fmt.kstr (fun err -> Error (`Msg err)) fmt

  module Infix = struct
    let ( >>= ) = ( >>= )
    let ( >|= ) = ( >|= )
    let return = return
    let fail = fail
  end

  module Syntax = struct
    let ( let* ) = ( let* )
    let ( let+ ) = ( let+ )
    let return = return
    let fail = fail
  end
end
