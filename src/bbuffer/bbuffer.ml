type inner_buffer = {buffer: Bstr.t; length: int}

type t = {
  mutable inner: inner_buffer;
  mutable position: int;
  initial_buffer: Bstr.t;
}
(* Invariants: all parts of the code preserve the invariants that:
   - [inner.length = Bytes.length inner.buffer]
   In absence of data races, we also have
   - [0 <= b.position <= b.inner.length]

   Note in particular that [b.position = b.inner.length] is legal,
   it means that the buffer is full and will have to be extended
   before any further addition. *)

let create n =
  let n = if n < 1 then 1 else n in
  let n = if n > Sys.max_string_length then Sys.max_string_length else n in
  let s = Bstr.create n in
  {inner= {buffer= s; length= n}; position= 0; initial_buffer= s}

let resize b more =
  let old_pos = b.position in
  let old_len = b.inner.length in
  let new_len = ref old_len in
  while old_pos + more > !new_len do
    new_len := 2 * !new_len
  done;
  if !new_len > Sys.max_string_length then begin
    if old_pos + more <= Sys.max_string_length then
      new_len := Sys.max_string_length
    else failwith "Buffer.add: cannot grow buffer"
  end;
  let new_buffer = Bstr.create !new_len in
  (* PR#6148: let's keep using [blit] rather than [unsafe_blit] in
     this tricky function that is slow anyway. *)
  Bstr.blit b.inner.buffer ~src_off:0 new_buffer ~dst_off:0 ~len:b.position;
  b.inner <- {buffer= new_buffer; length= !new_len}

let add_substring b s offset len =
  if offset < 0 || len < 0 || offset > String.length s - len then
    invalid_arg "Buffer.add_substring";
  let position = b.position in
  let {buffer; length} = b.inner in
  let new_position = position + len in
  if new_position > length then (
    resize b len;
    Bstr.blit_from_string s ~src_off:offset b.inner.buffer ~dst_off:b.position
      ~len)
  else Bstr.blit_from_string s ~src_off:offset buffer ~dst_off:position ~len;
  b.position <- new_position

let add_string b str = add_substring b str 0 (String.length str)

let sub b ofs len =
  if ofs < 0 || len < 0 || ofs > b.position - len then invalid_arg "Buffer.sub"
  else Bstr.sub b.inner.buffer ~off:ofs ~len

let length b = b.position

let blit src srcoff dst dstoff len =
  if
    len < 0
    || srcoff < 0
    || srcoff > src.position - len
    || dstoff < 0
    || dstoff > Bstr.length dst - len
  then invalid_arg "Buffer.blit"
  else Bstr.blit src.inner.buffer ~src_off:srcoff dst ~dst_off:dstoff ~len
