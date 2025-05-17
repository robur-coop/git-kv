let hdr = function
  | `Blob -> Fmt.str "blob %Ld\000"
  | `Tree -> Fmt.str "tree %Ld\000"
  | `Tag -> Fmt.str "tag %Ld\000"
  | `Commit -> Fmt.str "commit %Ld\000"

let ( % ) f g = fun x -> f (g x)

let sha1 =
  let open Digestif in
  let feed_bigstring bstr ctx = SHA1.feed_bigstring ctx bstr in
  let feed_bytes buf ~off ~len ctx = SHA1.feed_bytes ctx ~off ~len buf in
  {
    Carton.First_pass.feed_bytes;
    feed_bigstring;
    serialize= SHA1.to_raw_string % SHA1.get;
    length= SHA1.digest_size;
  }

let digest digest ctx fkind length serializer v =
  let open Carton.First_pass in
  let hdr = hdr fkind (length v) in
  let state = Encore.Lavoisier.emit v serializer in
  let rec go ctx = function
    | Encore.Lavoisier.Partial {buffer= str; off; len; continue} ->
      let ctx = digest.feed_bytes ~off ~len (Bytes.unsafe_of_string str) ctx in
      go ctx (continue ~committed:len)
    | Fail -> Fmt.failwith "Invalid Git object"
    | Done -> digest.serialize ctx |> Digestif.SHA1.of_raw_string
  in
  let ctx =
    digest.feed_bytes ~off:0 ~len:(String.length hdr)
      (Bytes.unsafe_of_string hdr)
      ctx
  in
  go ctx state
