# Target REST API

This is a [Singer](https://singer.io) target that sends JSON-formatted data to the REST API
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).


## Configuration

Example configuration with all possible configuration keys is below. The only required key is `api_url`. Other keys are optional.

``` json
{
    "api_url": "https://some_url.com/v1/endpoint",
    "disable_collection": true,
    "batch_size": null,
    "auth_token": null,
    "allowed_keys": ["a", "b"],
    "request_timeout": 10
}
```

Description of configuration keys:
* `api_url` - REST API endpoint URL. The only required configuration key.
* `disable_collection` - disable sending anonymous usage data to singer.io if set to `true`
* `batch_size` - number of JSON lines that should be send in one batch. If set to `null` or not specified the batch size is set to 1
* `auth_token` - authorization token
* `allowed_keys` - keys from input JSON that should be propagated to the REST API
* `request_timeout` - time for one request in seconds

## Testing

There are no automatic tests yet, but you can find the instructions for manual testing in the folder [test](test)
