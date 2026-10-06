import json
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import ( PipelineOptions, StandardOptions)
from apache_beam.io.gcp.bigquery import WriteToBigQuery




class FlattenPolicyEvent(beam.DoFn):
    def process(self, message):
        try:
            data = json.loads(message.decode("utf-8"))
            policy = data["session"]["data"]["policy"]
            line = policy["line"]
            risk = line["risk"][0]
            member = risk["member"]
            coverage = risk["coverage"][0]
            output = {
                "PROPOSAL_NUMBER": policy.get("PolicyNumber"),
                "INSURED_FIRST_INCEPTION_DATE": member.get("FirstInceptionDate"),
                "POL_INSURER_NAME": member.get("InsuredName"),
                "POL_INSURED_RELATION": member.get("RelationshipWithProposer"),
                "INSURED_GENDER": member.get("Gender"),
                "POL_INSURER_DOB": member.get("MemberDOB"),
                "POL_UNIQUE_MEMBER_ID": member.get("MemberID"),
                "POL_MEMBER_LEVEL_SI": member.get("SumInsured"),
                "POL_MEMBER_LEVEL_PREMIUM": risk.get("Premium"),
                "POL_COVERAGE_NAME": coverage.get("CoverageDescription"),
                "POL_COVERAGE_SI": coverage.get("SumInsured"),
                "POL_ACTUAL_PREMIUM": 0
            }

            yield output

        except Exception as e:
            logging.error( "Transformation failed: %s", str(e) )
            





BQ_SCHEMA = {
    "fields": [
        { "name": "PROPOSAL_NUMBER", "type": "STRING", "mode": "NULLABLE" },
        { "name": "INSURED_FIRST_INCEPTION_DATE", "type": "DATE", "mode": "NULLABLE" },
        { "name": "POL_INSURER_NAME", "type": "STRING", "mode": "NULLABLE" }, 
        { "name": "POL_INSURED_RELATION", "type": "STRING", "mode": "NULLABLE" }, 
        { "name": "INSURED_GENDER", "type": "STRING", "mode": "NULLABLE" }, 
        { "name": "POL_INSURER_DOB", "type": "DATE", "mode": "NULLABLE" }, 
        { "name": "POL_UNIQUE_MEMBER_ID", "type": "STRING", "mode": "NULLABLE" }, 
        { "name": "POL_MEMBER_LEVEL_SI", "type": "NUMERIC", "mode": "NULLABLE" }, 
        { "name": "POL_MEMBER_LEVEL_PREMIUM", "type": "NUMERIC", "mode": "NULLABLE" }, 
        { "name": "POL_COVERAGE_NAME", "type": "STRING", "mode": "NULLABLE" }, 
        { "name": "POL_COVERAGE_SI", "type": "NUMERIC", "mode": "NULLABLE" }, 
        { "name": "POL_ACTUAL_PREMIUM", "type": "NUMERIC", "mode": "NULLABLE" }
    ]
}





def run():
    options = PipelineOptions(
        runner="DataflowRunner",
        project="prj-srv-data-lake-prod60",
        region="asia-south1",
        streaming=True,
        save_main_session=True,
        temp_location="gs://hdfcergo-artifacts-prod/DC_realtime_raw/temp",
        staging_location="gs://hdfcergo-artifacts-prod/DC_realtime_raw/staging",
        job_name = f"duckcreek-pubsub-to-bq-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        dataflow_kms_key = "projects/prj-shared-common-srve0/locations/asia-south1/keyRings/kr-prod-prj-hdfc-lz-01/cryptoKeys/key-prj-srv-data-lake-prod60",
        service_account_email=f"data-lake-pre-prod-dataflow-sa@prj-srv-data-lake-prod60.iam.gserviceaccount.com",
        subnetwork=f"https://www.googleapis.com/compute/v1/projects/prj-shared-pre-prod-host60/regions/asia-south1/subnetworks/sb-as1-pre-prod-data-lake-dataflow",
        use_public_ips=False,

    )

    SUBSCRIPTION = "projects/prj-srv-prod-app-integrationfa/subscriptions/duckcreek_prod_policy_details-sub-v1"
    BQ_TABLE = "prj-srv-data-lake-prod60:hdfcergo_DC_raw_prod.DC_POLICY_LANDING_TABLE"

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "ReadFromPubSub"
            >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION
            )

            | "FlattenPolicyEvent"
            >> beam.ParDo(FlattenPolicyEvent())

            | "WriteToBigQuery"
            >> WriteToBigQuery(
                table=BQ_TABLE,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )




if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()