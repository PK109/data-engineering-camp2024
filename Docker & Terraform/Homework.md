
## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- **--rm** 


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- **0.42.0**
- 1.0.0
- 23.0.1
- 58.1.0


# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

Upload performed with `upload_data.ipynb`

## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
- **15612**
- 15859
- 89009
```
SELECT 
	COUNT(0)
FROM
	green_taxi_data
WHERE
	DATE(lpep_pickup_datetime) = '2019-09-18' 
		AND DATE(lpep_dropoff_datetime) = '2019-09-18'
```
## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every trip on a single day, we only care about the trip with the longest distance. 

- 2019-09-18
- 2019-09-16
- **2019-09-26**
- 2019-09-21

```
SELECT
	DATE(lpep_pickup_datetime)
FROM 
	green_taxi_data
WHERE
	trip_distance = (SELECT
						 MAX(trip_distance)
					  FROM
						 green_taxi_data)
```
## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
- **"Brooklyn" "Manhattan" "Queens"**
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"
```
SELECT z."Borough"
FROM public.green_taxi_data as td
	JOIN public.zones as z ON td."PULocationID" = z."LocationID" 
WHERE
	DATE(lpep_pickup_datetime) = '2019-09-18'
GROUP BY
	z."Borough"
HAVING
	SUM(total_amount)> 50000
```
## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- **JFK Airport**
- Long Island City/Queens Plaza

```
SELECT 
	z."Zone", td.tip_amount, td."PULocationID" 
FROM 
	public.green_taxi_data as td
	JOIN public.zones as z 
		ON td."DOLocationID" = z."LocationID" 
WHERE
	date_trunc('month', DATE(lpep_pickup_datetime)) = '2019-09-01'
	AND td."PULocationID" = 
		( 
		SELECT
			"LocationID"
		FROM 
			zones
		WHERE
			"Zone" = 'Astoria'
		)
ORDER BY
	td.tip_amount DESC
LIMIT 1
```

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.
```
google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_storage_bucket.demo-bucket: Creation complete after 2s [id=lateral-attic-426206-c4-terra-bucket]
google_bigquery_dataset.demo_dataset: Creation complete after 2s [id=projects/lateral-attic-426206-c4/datasets/demo_dataset]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET