# Mesh Constructor DSL
The Mesh Constructor DSL ([domain-specific language](https://en.wikipedia.org/wiki/Domain-specific_language))
makes it easy to write `Workflows` for the `constructor` package.

This DSL is most commonly used for writing automated Construction API
tests for the [`mesh-cli`](https://github.com/coinbase/mesh-cli#writing-checkconstruction-tests).

_Before reading more about the Mesh Constructor DSL, we recommend learning
about the frameworks used in the [`constructor`](/constructor/README.md)
to coordinate the creation of transactions._

## Syntax
At a basic level, the Mesh Constructor DSL syntax looks like this:
```text
// line comment
<workflow name>(<concurrency>){
  <scenario 1 name>{
    <output path> = <action type>(<input>); // another comment
  },
  <scenario 2 name>{
    <output path 2> = <action type>(<input>);
  }
}
```

### Basic Example
Here is a specific example for `Bitcoin`:
```text
request_funds(1){
  find_account{
    currency = {
      "symbol":"tBTC",
      "decimals":8
    };
    random_account = find_balance({
      "minimum_balance":{
        "value": "0",
        "currency": {{currency}}
      },
      "create_limit":1
    });
  },
  request{
    min_balance = load_env("MIN_BALANCE");
    adjusted_min = {{min_balance}} + 600;
    loaded_account = find_balance({
      "account_identifier": {{random_account.account_identifier}},
      "minimum_balance":{
        "value": {{adjusted_min}},
        "currency": {{currency}}
      }
    });
  }
}

create_account(1){
  create_account{
    network = {"network":"Testnet3", "blockchain":"Bitcoin"};
    key = generate_key({"curve_type":"secp256k1"});
    account = derive({
      "network_identifier": {{network}},
      "public_key": {{key.public_key}}
    });
    save_account({
      "account_identifier": {{account.account_identifier}},
      "keypair": {{key}}
    });
  }
}
```

If you were to write the same thing by hand in raw syntax, it
would look like:
```json
[
  {
    "name": "request_funds",
    "concurrency": 1,
    "scenarios": [
      {
        "name": "find_account",
        "actions": [
          {
            "input": "{\"symbol\":\"tBTC\", \"decimals\":8}",
            "type": "set_variable",
            "output_path": "currency"
          },
          {
            "input": "{\"minimum_balance\":{\"value\": \"0\", \"currency\": {{currency}}}, \"create_limit\":1}",
            "type": "find_balance",
            "output_path": "random_account"
          }
        ]
      },
      {
        "name": "request",
        "actions": [
          {
            "type": "load_env",
            "output_path": "min_balance",
            "input": "MIN_BALANCE"
          },
          {
            "type": "math",
            "ouput_path": "adjusted_min",
            "input": "{\"operation\":\"addition\", \"left_value\": {{min_balance}}, \"right_value\": \"600\"}"
          },
          {
            "input": "{\"account_identifier\": {{random_account.account_identifier}}, \"minimum_balance\":{\"value\": {{adjusted_min}}, \"currency\": {{currency}}}}",
            "type": "find_balance",
            "output_path": "loaded_account"
          }
        ]
      }
    ]
  },
  {
    "name": "create_account",
    "concurrency": 1,
    "scenarios": [
      {
        "name": "create_account",
        "actions": [
          {
            "input": "{\"network\":\"Testnet3\", \"blockchain\":\"Bitcoin\"}",
            "type": "set_variable",
            "output_path": "network"
          },
          {
            "input": "{\"curve_type\": \"secp256k1\"}",
            "type": "generate_key",
            "output_path": "key"
          },
          {
            "input": "{\"network_identifier\": {{network}}, \"public_key\": {{key.public_key}}}",
            "type": "derive",
            "output_path": "account"
          },
          {
            "input": "{\"account_identifier\": {{account.account_identifier}}, \"keypair\": {{key}}}",
            "type": "save_account"
          }
        ]
      }
    ]
  }
]
```
Note, if you plan to run the automated Construction API tester in CI for `create_account` workflow, you may wish to
provide [`prefunded accounts`](https://pkg.go.dev/github.com/coinbase/mesh-cli/configuration#ConstructionConfiguration)
when running the tester (otherwise you would need to manually fund generated
accounts).

### Workflows
`Workflows` are defined using the following syntax:
```text
<workflow name>(<concurrency>){
...
}
```

Note, `concurrency` must be provided when defining a `Workflow` and
no 2 `Workflows` can have the same name.

### Scenarios
`Scenarios` are defined using the following syntax:
```text
<workflow name>(<concurrency>){
  <scenario name>{
  ...
  }
}
```

`Scenarios` must be defined within a `Workflow` and no 2 `Scenarios`
in the same `Worfklow` can have the same name.

It is also important to note that `Workflows` containing multiple
`Scenarios` should be separated by a comma:
```text
<workflow name>(<concurrency>){
  <scenario name>{
  ...
  },
  <scenario name 2>{
  ...
  }
}
```

### Functions
In the Mesh Constructor DSL, it is possible to invoke functions (where
the function name is an `Action.Type`) but not possible to define your own
functions (yet!).

#### Inputs
The input for all functions is a JSON blob that will be evaluated by
the `Worker`. It is possible to reference other variables
in an input using the syntax `{{var}}` where `var` must follow
[this syntax](https://github.com/tidwall/gjson/blob/master/SYNTAX.md).
The Mesh Constructor DSL compiler will automatically check that referenced
variables are previously defined.

#### End Line
Function invocations can span multiple lines (if you "pretty print" the JSON
blob) but each function call line must end with a semi-colon.

#### Native Invocation
The Mesh Constructor DSL provides optional "native invocation" support for 2 `Action.Types`:
* `math`
* `set_variable`

"Native invocation" in this case means that the caller does not need to
invoke the `Action.Type` in the normal format:
```text
<output path> = <function name>(<input>);
```

##### math
`math` can be invoked by following the syntax:
```text
<output path> = <left side> <operator> <right side>;
```

A simple addition would look like:
```text
a = 10 + {{fee}};
```

Instead of:
```text
a = math({"operation":"addition","left_side":"10","right_side":{{fee}}});
```

##### set_variable
`set_variable` can be invoked by following the syntax:
```text
<output path> = <input>
```

A simple set would look like:
```text
a = {"message": "hello"};
```

Instead of:
```text
a = set_variable({"message": "hello"});
```

#### Recursive Calls
It is not possible to invoke a function from the input of another function. There
MUST be exactly 1 function call per line.

For example, this is not allowed:
```text
a = 1 + load_env("value");
```

### Comments
It is possible to add new line comments of comments at the end of lines
using a double slash (`//`).

## Status
The Mesh Constructor DSL should be considered `ALPHA` and may
include breaking changes in later releases. If you have any ideas on how to improve
the language, please
[open an issue in `mesh-sdk-go`](https://github.com/coinbase/mesh-sdk-go/issues)!
