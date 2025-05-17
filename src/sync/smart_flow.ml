open Lwt.Infix

let ( >>? ) = Lwt_result.bind

module Flow = struct
  open Rresult

  let blit0 src src_off dst dst_off len =
    let dst = Cstruct.of_bigarray ~off:dst_off ~len dst in
    Cstruct.blit src src_off dst 0 len

  let blit1 src src_off dst dst_off len =
    let src = Cstruct.of_bigarray ~off:src_off ~len src in
    Cstruct.blit src 0 dst dst_off len

  type t = {
    queue: (char, Bigarray.int8_unsigned_elt) Ke.Rke.t;
    flow: Mimic.flow;
  }

  type error = [ `Error of Mimic.error | `Write_error of Mimic.write_error ]

  let pp_error ppf = function
    | `Error err -> Mimic.pp_error ppf err
    | `Write_error err -> Mimic.pp_write_error ppf err

  let make flow = {flow; queue= Ke.Rke.create ~capacity:0x1000 Bigarray.char}

  let recv flow payload =
    if Ke.Rke.is_empty flow.queue then begin
      Mimic.read flow.flow >|= R.reword_error (fun err -> `Error err)
      >>? function
      | `Eof -> Lwt.return_ok `End_of_flow
      | `Data res ->
        Ke.Rke.N.push flow.queue ~blit:blit0 ~length:Cstruct.length res;
        let len = min (Cstruct.length payload) (Ke.Rke.length flow.queue) in
        Ke.Rke.N.keep_exn flow.queue ~blit:blit1 ~length:Cstruct.length ~off:0
          ~len payload;
        Ke.Rke.N.shift_exn flow.queue len;
        Lwt.return_ok (`Input len)
    end
    else
      let len = min (Cstruct.length payload) (Ke.Rke.length flow.queue) in
      Ke.Rke.N.keep_exn flow.queue ~blit:blit1 ~length:Cstruct.length ~len
        payload;
      Ke.Rke.N.shift_exn flow.queue len;
      Lwt.return_ok (`Input len)

  let send flow payload =
    Mimic.write flow.flow payload >|= function
    | Error `Closed -> R.error (`Write_error `Closed)
    | Error err -> R.error (`Write_error err)
    | Ok () -> R.ok (Cstruct.length payload)
end

let src = Logs.Src.create "smart.flow"

module Log = (val Logs.src_log src : Logs.LOG)

let run : Flow.t -> ('res, [ `Protocol of Smart.error ]) Smart.t -> 'res Lwt.t =
 fun flow fiber ->
  let ( >>= ) = Lwt.bind in
  let tmp = Cstruct.create 65535 in
  let failwithf fmt = Format.kasprintf (fun err -> raise (Failure err)) fmt in
  let rec go = function
    | Smart.Read {k; buffer; off; len; eof} -> (
      let max = Int.min (Cstruct.length tmp) len in
      Flow.recv flow (Cstruct.sub tmp 0 max) >>= function
      | Ok `End_of_flow -> go (eof ())
      | Ok (`Input len) ->
        Cstruct.blit_to_bytes tmp 0 buffer off len;
        Log.debug (fun m ->
            m "-> @[<hov>%a@]"
              (Hxd_string.pp Hxd.default)
              (Bytes.sub_string buffer off len));
        go (k len)
      | Error err -> failwithf "%a" Flow.pp_error err)
    | Smart.Write {k; buffer; off; len} ->
      Log.debug (fun m ->
          m "<- @[<hov>%a@]"
            (Hxd_string.pp Hxd.default)
            (String.sub buffer off len));
      let rec loop tmp =
        if Cstruct.length tmp = 0 then go (k len)
        else begin
          Flow.send flow tmp >>= function
          | Ok shift -> loop (Cstruct.shift tmp shift)
          | Error err -> failwithf "%a" Flow.pp_error err
        end
      in
      loop (Cstruct.of_string buffer ~off ~len)
    | Smart.Return v -> Lwt.return v
    | Smart.Error (`Protocol err) -> failwithf "%a" Smart.pp_error err
  in
  go fiber
