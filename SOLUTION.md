# Solution for DataEngineerChallenge

## Environment

### Solution Framework

- Spark (v2.4.0): Spark SQL Scala API

### Development Environment

- Prerequisite: Docker Compose
- Refer: Spark Cluster is built with [big-data-europe/docker-spark](https://github.com/big-data-europe/docker-spark)

### Dependent Library

- Sessionization: [spark-udwf-session](https://github.com/weasellin/spark-udwf-session)

## Project Usage

### Init

```
$ git clone --recursive --branch spark https://github.com/weasellin/DataEngineerChallenge.git
$ cd DataEngineerChallenge
```

### Build

```
$ docker-compose build
```

### Test

```
$ docker-compose run spark-driver sbt test
```

### Run

```
$ docker-compose up -d
$ docker-compose logs -f spark-driver
```

## Result

### Average Session Time

```
$ cat data/output/session_duration_avg/*.csv

211.20631982892198 (Seconds)
```

### Unique URL Visits per Session

```
$ echo "Count   User Session" \
  && echo "=========================================================================================================" \
  && cat data/output/session/*.csv \
  | awk -F',' '{print $7"\t"$2" "$1}'

Count   User Session
=========================================================================================================
1       52.74.219.71:33804;Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html) 0001fa76-0a6e-461c-a175-e64154880f2c
1       "54.169.192.103:40189;Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML 00033ab1-34f1-486a-bc03-ba36ed28e300
5       "113.193.21.16:51363;Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML 000342d5-d48c-4997-8992-4048e26cfd63
1       "54.169.106.125:34051;Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML 0003640e-bedf-4ad4-b996-20d63e94281a
1       "106.220.128.26:1043;Mozilla/5.0 (iPhone; CPU iPhone OS 8_4 like Mac OS X) AppleWebKit/600.1.4 (KHTML 0005956b-e0ab-48b1-9f9d-b71f3e4b02d3
2       "117.197.205.58:57539;Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML 000753b7-c73d-4b00-8461-6fbc28234d58
1       "117.192.107.226:61603;Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML 00075635-5b6b-47a5-beeb-4f10ce02d49a
1       "121.244.199.210:22227;Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML 000947dc-5cd1-4f68-b286-4d48543bbb8a
1       "119.81.61.166:43818;Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML 0009ba32-b3c3-402a-956b-aae547ff850b
1       27.97.161.156:61730;Mozilla/5.0 (Windows NT 6.1; rv:39.0) Gecko/20100101 Firefox/39.0 000edd54-2e68-49e0-991e-6938d08724bf

```

### Most Engaged Users

```
$ echo "session_id, user_id, start, end, duration, visit, uniqe visit" \
  && cat data/output/session_longest/*.csv

session_id, user_id, start, end, duration, visit, uniqe visit
6bced95b-697a-402e-96df-2b32c94325c5,103.29.159.138:57045;Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20100101 Firefox/21.0,2015-07-22T10:30:57.519Z,2015-07-22T11:21:04.526Z,3007,96,3
e74808e8-dfd0-4ff2-8ff8-a7986d7ebc84,122.169.141.4:11486;Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0,2015-07-22T10:31:24.947Z,2015-07-22T11:21:26.263Z,3002,152,10
affa92b6-fdd0-4237-9e65-aed8f59dd3eb,122.169.141.4:50427;Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0,2015-07-22T10:31:26.496Z,2015-07-22T11:21:11.529Z,2985,153,9
...
```
