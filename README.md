# echo-adl
ADL for EchoCare

**Example for running both daily and mpnthly adl and automatically gets the relevant dates**
```shell
python cloud_api.py --device_id D0:63:B4:02:3B:C8 
```

**Example for running daily adl:**
```shell
python cloud_api.py --mode day --device_id D0:63:B4:02:3B:C8 --from_date 2023-07-01 --to_date 2023-07-31​
```

**Example for running monthly adl:**
```shell
python cloud_api.py --mode month --device_id D0:63:B4:02:3B:C8 --from_date 2023-07-01 --to_date 2023-07-31​
```