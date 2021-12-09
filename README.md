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

## TODOs

* **Handle server authentification** 
* **Handle bad URL and issues with REST server**
* **Make batching safer** It is probably fine now but someone should check it
* **Implement reaction to singer message [ActivateVersionMessage](https://github.com/singer-io/singer-python/blob/0c066de21111d8572425083b4a8792d193c80af1/singer/messages.py#L137)** Now it is just ignored but to implement this could be tricky. We would need specific enpoint that will delete all previously send records and check record version.
* **Think about better way to handle the last batch** that is smaller then batch_size (for cases when data_size % batch_size != 0)
* **Better handling of REST server response codes < 400** For example response codes like `204 - No content` is treated like everithing is fine but it can mean some problems
