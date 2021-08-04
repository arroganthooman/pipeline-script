try:
    import io
    from io import BytesIO
    from google.cloud import storage

except Exception as e:
    print("Some Modules are Missing {}".format(e))


#put the service account name, json format
storage_client = storage.Client.from_service_account_json("E:/wired-glider-289003-9bd74e62ec18.json")

#create a Bucket object
bucket = storage_client.get_bucket("bucket-lingga")

# GCP Bucket file name
# filename = "%s/%s" % ('', "MYY.csv")
filename = 'CompareKPIsByDate.xlsx'
blob = bucket.blob(filename)

# File name that will be upload
blob.upload_from_filename('D:/sb_source_xlxs/CompareKPIsByDate.xlsx')
print("File uploaded")

# namefile = ["CompareKPIsByDate", "DiscDailyDetail", "cherry"]
# for x in namefile:
#   print(x)