# Rosetta Constructor DSL
The Rosetta Constructor DSL ([domain-specific language](https://en.wikipedia.org/wiki/Domain-specific_language))
makes it easy



to define `Workflows`, `Scenarios`, and `Actions`

the `constructor` to create transactions on any
blockchain that implements the Rosetta Construction API.

_Before reading about the Rosetta Constructor DSL, we recommend refreshing
your understanding of how the entire Constructor fits together._

### Syntax
At a basic level, the Rosetta Constructor DSL looks like this:
```text
<workflow name>(<concurrency>){
  <scenario 1 name>{
    <output path> = <action type>(<input>);
  },
  <scenario 2 name>{
    <output path 2> = <action type>(<input>);
  }
}
```

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

### Functions

#### Input
JSON input into constructor

#### Native Invocation
* `math`
* `set_variable`

### Status
The Rosetta Constructor DSL should be considered `ALPHA` and may
still include breaking changes. If you have any ideas of how to improve
the language, open an issue!

### Parser Features
* Ensure you aren't using any invalid variables

