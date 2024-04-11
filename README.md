# Finance series prediction using news

## Deploy

1. Create bucket
1. Create service account with role `storage.admin`
1. Create static key. Create file `~/.aws/credentials` with the following content:
```
[default]
  aws_access_key_id = <access-key-id>
  aws_secret_access_key = <secret-access-key>
```
Then create file `~/.aws/config` with the following content:
```
[default]
  region=ru-central1
```
