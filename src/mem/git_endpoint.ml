type handshake = uri0:Uri.t -> uri1:Uri.t -> Mimic.flow -> unit Lwt.t

let git_capabilities : [ `Rd | `Wr ] Mimic.value =
  Mimic.make ~name:"git-capabilities"

let git_scheme :
    [ `Git | `SSH | `HTTP | `HTTPS | `Scheme of string ] Mimic.value =
  Mimic.make ~name:"git-scheme"

let git_path = Mimic.make ~name:"git-path"
let git_hostname = Mimic.make ~name:"git-hostname"
let git_ssh_user = Mimic.make ~name:"git-ssh-user"
let git_port = Mimic.make ~name:"git-port"
let git_http_headers = Mimic.make ~name:"git-http-headers"

let git_transmission : [ `Git | `Exec | `HTTP of Uri.t * handshake ] Mimic.value
    =
  Mimic.make ~name:"git-transmission"

let git_uri = Mimic.make ~name:"git-uri"

type t = {
  scheme:
    [ `SSH of string
    | `Git
    | `HTTP of (string * string) list
    | `HTTPS of (string * string) list
    | `Scheme of string ];
  port: int option;
  path: string;
  hostname: string;
}

let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt
let msgf fmt = Fmt.kstr (fun msg -> `Msg msg) fmt

let pp ppf edn =
  match edn with
  | {scheme= `SSH user; path; hostname; _} ->
    Fmt.pf ppf "%s@%s:%s" user hostname path
  | {scheme= `Git; port; path; hostname} ->
    Fmt.pf ppf "git://%s%a/%s" hostname
      Fmt.(option (const string ":" ++ int))
      port path
  | {scheme= `HTTP _; path; port; hostname} ->
    Fmt.pf ppf "http://%s%a%s" hostname
      Fmt.(option (const string ":" ++ int))
      port path
  | {scheme= `HTTPS _; path; port; hostname} ->
    Fmt.pf ppf "https://%s%a%s" hostname
      Fmt.(option (const string ":" ++ int))
      port path
  | {scheme= `Scheme scheme; path; port; hostname} ->
    Fmt.pf ppf "%s://%s%a/%s" scheme hostname
      Fmt.(option (const string ":" ++ int))
      port path

let headers_from_uri uri =
  match Uri.user uri, Uri.password uri with
  | Some user, Some password ->
    let raw = Base64.encode_exn (Fmt.str "%s:%s" user password) in
    ["Authorization", Fmt.str "Basic %s" raw]
  | _ -> []

let of_string str =
  let ( >>= ) = Result.bind in
  let parse_ssh str =
    let len = String.length str in
    Emile.of_string_raw ~off:0 ~len str
    |> Result.map_error (msgf "%a" Emile.pp_error)
    >>= fun (consumed, m) ->
    match
      String.split_on_char ':' (String.sub str consumed (len - consumed))
    with
    | "" :: path ->
      let path = String.concat ":" path in
      let local =
        List.map
          (function `Atom x -> x | `String x -> Fmt.str "%S" x)
          m.Emile.local
      in
      let user = String.concat "." local in
      let hostname =
        match fst m.Emile.domain with
        | `Domain vs -> String.concat "." vs
        | `Literal v -> v
        | `Addr (Emile.IPv4 v) -> Ipaddr.V4.to_string v
        | `Addr (Emile.IPv6 v) -> Ipaddr.V6.to_string v
        | `Addr (Emile.Ext (k, v)) -> Fmt.str "%s:%s" k v
      in
      Ok {scheme= `SSH user; path; port= None; hostname}
    | _ -> Error (`Msg "Invalid SSH pattern")
  in
  let parse_uri str =
    let uri = Uri.of_string str in
    let path = Uri.path uri in
    match Uri.scheme uri, Uri.host uri, Uri.port uri with
    | Some "git", Some hostname, port -> Ok {scheme= `Git; path; port; hostname}
    | Some "http", Some hostname, port ->
      Ok {scheme= `HTTP (headers_from_uri uri); path; port; hostname}
    | Some "https", Some hostname, port ->
      Ok {scheme= `HTTPS (headers_from_uri uri); path; port; hostname}
    | Some scheme, Some hostname, port ->
      Ok {scheme= `Scheme scheme; path; port; hostname}
    | _ -> error_msgf "Invalid uri: %a" Uri.pp uri
  in
  match parse_ssh str, parse_uri str with
  | Ok v, _ -> Ok v
  | _, Ok v -> Ok v
  | Error _, Error _ ->
    error_msgf
      "Invalid endpoint: %s\n\
       The format of it corresponds to:\n\
       1) a SSH endpoint like: user@hostname:repository\n\
       2) a Git endpoint like: git://hostname(:port)?/repository\n\
       3) a HTTP endpoint like: \
       http(s)?://(user:password@)?hostname(:port)?/repository\n\
       4) an URI with a special scheme like: \
       [scheme]://hostname(:port)?/repository\n\n\
       It's not possible to specify a port if you use an SSH endpoint and it's \
       not\n\
       possible to specify an user and its password if you use a Git or an URI \
       with\n\
       a special scheme endpoint."
      str

let with_headers_if_http headers ({scheme; _} as edn) =
  match scheme with
  | `SSH _ | `Git | `Scheme _ -> edn
  | `HTTP _ -> {edn with scheme= `HTTP headers}
  | `HTTPS _ -> {edn with scheme= `HTTPS headers}

let to_ctx edn ctx =
  let scheme =
    match edn.scheme with
    | `Git -> `Git
    | `SSH _ -> `SSH
    | `HTTP _ -> `HTTP
    | `HTTPS _ -> `HTTPS
    | `Scheme scheme -> `Scheme scheme
  in
  let headers =
    match edn.scheme with
    | `HTTP headers | `HTTPS headers -> Some headers
    | _ -> None
  in
  let ssh_user = match edn.scheme with `SSH user -> Some user | _ -> None in
  (* XXX(dinosaure): I don't like the reconstruction of the given [Uri.t] when we can miss some informations. *)
  let uri =
    match edn.scheme with
    | `HTTP _ ->
      Some
        (Uri.of_string
           (Fmt.str "http://%s%a%s" edn.hostname
              Fmt.(option (const string ":" ++ int))
              edn.port edn.path))
    | `HTTPS _ ->
      Some
        (Uri.of_string
           (Fmt.str "https://%s%a%s" edn.hostname
              Fmt.(option (const string ":" ++ int))
              edn.port edn.path))
    | _ -> None
  in
  ctx
  |> Mimic.add git_scheme scheme
  |> Mimic.add git_path edn.path
  |> Mimic.add git_hostname edn.hostname
  |> fun ctx ->
  Option.fold ~none:ctx ~some:(fun v -> Mimic.add git_ssh_user v ctx) ssh_user
  |> fun ctx ->
  Option.fold ~none:ctx ~some:(fun v -> Mimic.add git_port v ctx) edn.port
  |> fun ctx ->
  Option.fold ~none:ctx ~some:(fun v -> Mimic.add git_uri v ctx) uri
  |> fun ctx ->
  Option.fold ~none:ctx
    ~some:(fun v -> Mimic.add git_http_headers v ctx)
    headers
