# Constructor

[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=shield)](https://pkg.go.dev/github.com/coinbase/mesh-sdk-go/constructor?tab=doc)

The `constructor` package is used for coordinating the construction
and broadcast of transactions on any blockchain that implements the
Mesh API. It was designed to power automated Construction API
testing in the [`mesh-cli (check:construction)`](https://github.com/coinbase/mesh-cli#checkconstruction-1)
but could be useful for anyone building a Mesh API wallet.

## Framework
When first learning about a new topic, it is often useful to understand the
hierarchy of concerns. In the `constructor`, this "hierarchy" is as follows:
```text
Workflows -> Jobs
  Scenarios
    Actions
```

`Workflows` contain collections of `Scenarios` to execute. `Scenarios` are
executed atomically in database transactions (rolled back if execution fails)
and culminate in an optional broadcast. This means that a single `Workflow`
could contain multiple broadcasts (which can be useful for orchestrating
staking-related transactions that affect a single account).

To perform a `Workflow`, we create a `Job`. This `Job` has a unique identifier
and stores state for all `Scenarios` in the `Workflow`. State is shared across
an entire `Job` so `Actions` in a `Scenario` can access the output of `Actions`
in other `Scenarios`. The syntax for accessing this shared state can be found
[here](https://github.com/tidwall/gjson/blob/master/SYNTAX.md).

`Actions` are discrete operations that can be performed in the context of a
`Scenario`.  A full list of all `Actions` that can be performed can be found
[here](https://pkg.go.dev/github.com/coinbase/mesh-sdk-go/constructor/job#ActionType).

If you have suggestions for more actions, please
[open an issue in `mesh-sdk-go`](https://github.com/coinbase/mesh-sdk-go/issues)!

### Broadcast Invocation
If you'd like to broadcast a transaction at the end of a `Scenario`,
you must populate the following fields:
* `<scenario>.network`
* `<scenario>.operations`
* `<scenario>.confirmation_depth` (allows for stake-related transactions to complete before marking as a success)

Optionally, you can populate the following field:
* `<scenario>.preprocess_metadata`

Once a transaction is confirmed on-chain (after the provided
`<scenario>.confirmation_depth`, it is stored by the tester at
`<scenario>.transaction` for access by other `Scenarios` in the same `Job`.

### Dry Runs
In UTXO-based blockchains, it may be necessary to amend the `operations` stored
in `<scenario>.operations` based on the `suggested_fee` returned in
`/construction/metadata`. The `constructor` supports running a "dry run" of
a transaction broadcast if you set the follow field:
* `<scenario>.dry_run = true`

The suggested fee will then be stored as `<scenario>.suggested_fee` for use by
other `Scenarios` in the same `Job`. You can find an example of this in the
[Ethereum configuration](https://github.com/coinbase/mesh-ethereum/blob/master/mesh-cli-conf/testnet/ethereum.ros).

*If this field is not populated or set to `false`, the transaction
will be constructed, signed, and broadcast.*

### Using with mesh-cli
If you use the `constructor` for automated Construction API testing (without prefunded
accounts), you MUST implement 2 required `Workflows`:
* `create_account`
* `request_funds`

_If you don't implement these 2 `Workflows`, processing could stall._

Please note that `create_account` can contain a transaction broadcast if
on-chain origination is required for new accounts on your blockchain.

If you plan to run the `constructor` in CI, you may wish to
provide [`prefunded accounts`](https://pkg.go.dev/github.com/coinbase/mesh-cli/configuration#ConstructionConfiguration)
when running the tester (otherwise you would need to manually fund generated
accounts).

Optionally, you can also provide a `return_funds` workflow that will be invoked
when exiting `check:construction`. This can be useful in CI when you want to return
all funds to a single accout or faucet (instead of black-holing them in all the addresses
created during testing).

### Writing Workflows
It is possible to write `Workflows` from scratch using JSON, however, it is
highly recommended to use the [Mesh Constructor DSL](dsl/README.md). You can
see an example of how these two approaches compare below:

#### Without DSL
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

#### With DSL
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

### Future Work
* Create a `wallet` package that uses `Workflows` as core logic
  * Requests made to the `wallet` could be injected into the `Workflow`
  state before starting execution to enable user-provided parameters
* Support the creation of modular functions that can be reused across
`Workflows`
  * The functions would likely maintain their own state and just require
  some collection of inputs to be defined when they start execution and allow
  for mapping results back to the `Workflow` state when finished.
* Develop a testing suite that allows `Workflow` writers to test their scripts
  * Often times, the development cycle for working with `Workflows` is to write
  and then test on a live network. It should be possible to mock `Actions` and ensure
  a set of `Operations` are created.
