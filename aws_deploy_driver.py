import boto3
import json
import os

# TODO: Uses the Access configured in ~/.aws/credentials file
# TODO: Get Region as from the file - Also enable supply key and id from config file


class Driver:
    """
    Class to driver the whole process
    The class heavily uses boto3
    Refer http://boto3.readthedocs.io/en/latest/guide/quickstart.html for details
    """

    json_config_filename = 'config.json'
    key_for_relative_path_to_jar = 'relative_path_to_jar'
    key_for_s3_bucket_name = 's3_bucket_name'
    key_for_lambda_function_name = 'lambda_function_name'

    @staticmethod
    def upload_jar_to_s3(path_to_jar, bucket_name):
        """
        This Method uploads a jar to S3 bucket
        :param path_to_jar: 
        :param bucket_name
        :return: Bucket Name and Object Key for uploaded Jar
        """
        s3 = boto3.resource('s3')
        # TODO: Add more logging to indicate the upload status
        key = os.path.basename(path_to_jar)
        s3.meta.client.upload_file(path_to_jar, bucket_name, key, ExtraArgs={'ACL':'authenticated-read'})
        return bucket_name, key

    @staticmethod
    def read_json_config(json_file_path):
        """
        Read the JSON File and load Config
        :return: dynamic json config
        """
        json_config = ''
        with open(json_file_path) as json_data:
            json_config = json.load(json_data)
        return json_config

    @staticmethod
    def update_lambda_with_jar(lambda_function_name, s3_bucket_name, s3_object_key):
        """
        update an existing lambda with a JAR
        :return: function ARN
        """
        key_for_function_arn = 'FunctionArn'
        lambda_client = boto3.client('lambda')
        response = lambda_client.update_function_code(FunctionName=lambda_function_name,S3Bucket=s3_bucket_name,S3Key=s3_object_key)
        return response

    def execute(self):
        """
        drive the complete implementation
        :return: 
        """
        function_arn = []
        # TODO: Better logging to indicate the number of configs processed and show to the user.
        config_values = self.read_json_config(self.json_config_filename)
        for config in config_values['config']:
            bucket_name, object_key = self.upload_jar_to_s3(config[self.key_for_relative_path_to_jar],
                                                            config[self.key_for_s3_bucket_name])
            for lambda_function_name in config[self.key_for_lambda_function_name]:
                function_arn.append(self.update_lambda_with_jar(lambda_function_name, bucket_name, object_key))
        print('The number of ARNs updated')
        print(function_arn)


if __name__ == '__main__':
    driver = Driver()
    driver.execute()
