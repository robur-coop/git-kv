(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 * and Romain Calascibetta <romain.calascibetta@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

let src = Logs.Src.create "git.blob" ~doc:"logs git's blob event"

module Log = (val Logs.src_log src : Logs.LOG)
module SHA1 = Digestif.SHA1

type t = Bstr.t

let of_string x : t = Bstr.of_string x
let to_string (x : t) = Bstr.to_string x
let length : t -> int64 = fun t -> Int64.of_int (Bstr.length t)

let digest bstr =
  let ctx = SHA1.empty in
  let hdr = Fmt.str "blob %Ld\000" (length bstr) in
  let ctx = SHA1.feed_string ctx hdr in
  let ctx = SHA1.feed_bigstring ctx bstr in
  SHA1.get ctx

let pp ppf blob = Fmt.string ppf (Bstr.to_string blob)
let equal = Bstr.equal
let compare = Bstr.compare
let hash = Hashtbl.hash

module Set = Set.Make (struct
  type nonrec t = t

  let compare = compare
end)

module Map = Map.Make (struct
  type nonrec t = t

  let compare = compare
end)
