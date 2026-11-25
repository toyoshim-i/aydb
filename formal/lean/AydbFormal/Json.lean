set_option autoImplicit false

/-!
  Pure `Init`-only JSON parser and typed combinators for Schema v1 NDJSON trace
  verification (`aydb-trace-check`), with zero `import Lean`, zero `partial`,
  zero `unsafe`, and zero `sorry`.
-/

namespace AydbFormal.Json

inductive Json where
  | null
  | bool (b : Bool)
  | int (n : Int)
  | str (s : String)
  | arr (xs : List Json)
  | obj (kvs : List (String × Json))
  deriving Repr, BEq, Inhabited

private def isWs (c : Char) : Bool :=
  c == ' ' || c == '\t' || c == '\n' || c == '\r'

private def skipWs : List Char → List Char
  | [] => []
  | c :: cs => if isWs c then skipWs cs else c :: cs

private def isDigit (c : Char) : Bool :=
  '0' ≤ c && c ≤ '9'

private def hexVal (c : Char) : Option Nat :=
  if '0' ≤ c && c ≤ '9' then some (c.toNat - '0'.toNat)
  else if 'a' ≤ c && c ≤ 'f' then some (10 + c.toNat - 'a'.toNat)
  else if 'A' ≤ c && c ≤ 'F' then some (10 + c.toNat - 'A'.toNat)
  else none

private def parseStringAux : Nat → List Char → List Char → Except String (String × List Char)
  | 0, _, _ => .error "json string parser ran out of fuel"
  | _ + 1, [], _ => .error "unterminated json string"
  | _ + 1, '"' :: rest, acc => .ok (String.ofList acc.reverse, rest)
  | fuel + 1, '\\' :: rest, acc =>
      match rest with
      | [] => .error "unterminated escape sequence in json string"
      | '"' :: tail => parseStringAux fuel tail ('"' :: acc)
      | '\\' :: tail => parseStringAux fuel tail ('\\' :: acc)
      | '/' :: tail => parseStringAux fuel tail ('/' :: acc)
      | 'b' :: tail => parseStringAux fuel tail (Char.ofNat 8 :: acc)
      | 'f' :: tail => parseStringAux fuel tail (Char.ofNat 12 :: acc)
      | 'n' :: tail => parseStringAux fuel tail ('\n' :: acc)
      | 'r' :: tail => parseStringAux fuel tail ('\r' :: acc)
      | 't' :: tail => parseStringAux fuel tail ('\t' :: acc)
      | 'u' :: h1 :: h2 :: h3 :: h4 :: tail =>
          match hexVal h1, hexVal h2, hexVal h3, hexVal h4 with
          | some d1, some d2, some d3, some d4 =>
              let code := ((d1 * 16 + d2) * 16 + d3) * 16 + d4
              parseStringAux fuel tail (Char.ofNat code :: acc)
          | _, _, _, _ => .error "invalid \\uXXXX escape in json string"
      | other :: _ => .error s!"unsupported escape '\\{other}' in json string"
  | fuel + 1, c :: rest, acc =>
      if c.toNat < 32 then
        .error "unescaped control character in json string"
      else
        parseStringAux fuel rest (c :: acc)

private def parseNatDigitsAux : Nat → List Char → Nat → (Nat × List Char)
  | 0, cs, acc => (acc, cs)
  | fuel + 1, c :: rest, acc =>
      if isDigit c then
        let d := c.toNat - '0'.toNat
        parseNatDigitsAux fuel rest (acc * 10 + d)
      else
        (acc, c :: rest)
  | _ + 1, [], acc => (acc, [])

private def parseInt (fuel : Nat) (cs : List Char) : Except String (Int × List Char) :=
  let (neg, digitsStart) :=
    match cs with
    | '-' :: rest => (true, rest)
    | _ => (false, cs)
  match digitsStart with
  | c :: _ =>
      if !isDigit c then
        .error "expected digit in json integer"
      else
        let (mag, rest) := parseNatDigitsAux fuel digitsStart 0
        match rest with
        | '.' :: _ => .error "floating-point json numbers are unsupported in schema v1"
        | 'e' :: _ => .error "exponent json numbers are unsupported in schema v1"
        | 'E' :: _ => .error "exponent json numbers are unsupported in schema v1"
        | _ =>
            let val : Int := Int.ofNat mag
            .ok (if neg then -val else val, rest)
  | [] => .error "unexpected end of input in json integer"

mutual
  private def parseValueAux : Nat → List Char → Except String (Json × List Char)
    | 0, _ => .error "json parser ran out of fuel"
    | fuel + 1, cs =>
        match skipWs cs with
        | [] => .error "unexpected end of json input"
        | 'n' :: 'u' :: 'l' :: 'l' :: rest => .ok (.null, rest)
        | 't' :: 'r' :: 'u' :: 'e' :: rest => .ok (.bool true, rest)
        | 'f' :: 'a' :: 'l' :: 's' :: 'e' :: rest => .ok (.bool false, rest)
        | '"' :: rest => do
            let (s, tail) ← parseStringAux (fuel + 1) rest []
            return (.str s, tail)
        | '[' :: rest =>
            match skipWs rest with
            | ']' :: tail => .ok (.arr [], tail)
            | other => parseArrayElems fuel other []
        | '{' :: rest =>
            match skipWs rest with
            | '}' :: tail => .ok (.obj [], tail)
            | other => parseObjectPairs fuel other []
        | c :: rest =>
            if c == '-' || isDigit c then do
              let (n, tail) ← parseInt (fuel + 1) (c :: rest)
              return (.int n, tail)
            else
              .error s!"unexpected token starting with '{c}' in json input"

  private def parseArrayElems : Nat → List Char → List Json → Except String (Json × List Char)
    | 0, _, _ => .error "json array parser ran out of fuel"
    | fuel + 1, cs, acc => do
        let (elem, afterElem) ← parseValueAux fuel cs
        let acc' := elem :: acc
        match skipWs afterElem with
        | ',' :: rest => parseArrayElems fuel rest acc'
        | ']' :: rest => .ok (.arr acc'.reverse, rest)
        | other =>
            match other with
            | [] => .error "unterminated json array"
            | c :: _ => .error s!"expected ',' or ']' in json array, found '{c}'"

  private def parseObjectPairs :
      Nat → List Char → List (String × Json) → Except String (Json × List Char)
    | 0, _, _ => .error "json object parser ran out of fuel"
    | fuel + 1, cs, acc =>
        match skipWs cs with
        | '"' :: rest => do
            let (key, afterKey) ← parseStringAux (fuel + 1) rest []
            match skipWs afterKey with
            | ':' :: afterColon => do
                let (val, afterVal) ← parseValueAux fuel afterColon
                let acc' := (key, val) :: acc
                match skipWs afterVal with
                | ',' :: tail => parseObjectPairs fuel tail acc'
                | '}' :: tail => .ok (.obj acc'.reverse, tail)
                | [] => .error "unterminated json object"
                | c :: _ => .error s!"expected ',' or '}}' in json object, found '{c}'"
            | [] => .error "expected ':' after json object key, found EOF"
            | c :: _ => .error s!"expected ':' after json object key, found '{c}'"
        | [] => .error "expected string key in json object, found EOF"
        | c :: _ => .error s!"expected string key in json object, found '{c}'"
end

def parseJson (input : String) : Except String Json := do
  let chars := input.toList
  let fuel := (chars.length + 2) * 4
  let (val, rest) ← parseValueAux fuel chars
  match skipWs rest with
  | [] => .ok val
  | c :: _ => .error s!"trailing characters after json value starting at '{c}'"

def reqField (j : Json) (key : String) : Except String Json :=
  match j with
  | .obj kvs =>
      match kvs.find? (fun kv => kv.1 == key) with
      | some (_, v) => .ok v
      | none => .error s!"missing required json field `{key}`"
  | _ => .error s!"expected json object when looking up `{key}`"

def asStr : Json → Except String String
  | .str s => .ok s
  | _ => .error "expected json string"

def asBool : Json → Except String Bool
  | .bool b => .ok b
  | _ => .error "expected json bool"

def asInt : Json → Except String Int
  | .int n => .ok n
  | _ => .error "expected json integer"

def asNat : Json → Except String Nat
  | .int n =>
      if n ≥ 0 then .ok n.toNat
      else .error s!"expected non-negative json integer, got {n}"
  | _ => .error "expected non-negative json integer"

def asOptNat : Json → Except String (Option Nat)
  | .null => .ok none
  | .int n =>
      if n ≥ 0 then .ok (some n.toNat)
      else .error s!"expected null or non-negative json integer, got {n}"
  | _ => .error "expected null or non-negative json integer"

def asArr : Json → Except String (List Json)
  | .arr xs => .ok xs
  | _ => .error "expected json array"

end AydbFormal.Json
