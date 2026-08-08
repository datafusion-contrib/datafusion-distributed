import * as aws from '@pulumi/aws';
import * as pulumi from '@pulumi/pulumi';

import { FoundationConfig } from './config';

export interface BenchmarkStorage {
  datasetBucket: aws.s3.Bucket;
  resultsBucket: aws.s3.Bucket;
}

function createProtectedBucket(name: string, bucketPrefix: string): aws.s3.Bucket {
  const bucket = new aws.s3.Bucket(
    name,
    {
      bucketPrefix,
      forceDestroy: true,
      tags: {
        Name: bucketPrefix.replace(/-$/, ''),
        'benchmark.datafusion.apache.org/lifecycle': 'retained',
      },
    },
    { protect: true },
  );

  new aws.s3.BucketOwnershipControls(
    `${name}-ownership`,
    {
      bucket: bucket.id,
      rule: {
        objectOwnership: 'BucketOwnerEnforced',
      },
    },
    { protect: true },
  );

  new aws.s3.BucketPublicAccessBlock(
    `${name}-public-access`,
    {
      bucket: bucket.id,
      blockPublicAcls: true,
      blockPublicPolicy: true,
      ignorePublicAcls: true,
      restrictPublicBuckets: true,
    },
    { protect: true },
  );

  new aws.s3.BucketVersioning(
    `${name}-versioning`,
    {
      bucket: bucket.id,
      versioningConfiguration: {
        status: 'Enabled',
      },
    },
    { protect: true },
  );

  new aws.s3.BucketServerSideEncryptionConfiguration(
    `${name}-encryption`,
    {
      bucket: bucket.id,
      rules: [
        {
          applyServerSideEncryptionByDefault: {
            sseAlgorithm: 'AES256',
          },
          bucketKeyEnabled: true,
        },
      ],
    },
    { protect: true },
  );

  new aws.s3.BucketPolicy(
    `${name}-tls-policy`,
    {
      bucket: bucket.id,
      policy: pulumi.interpolate`{
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "DenyInsecureTransport",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": ["${bucket.arn}", "${bucket.arn}/*"],
                "Condition": {"Bool": {"aws:SecureTransport": "false"}}
            }]
        }`,
    },
    { protect: true },
  );

  return bucket;
}

export function createStorage(config: FoundationConfig): BenchmarkStorage {
  const datasetBucket = createProtectedBucket(
    'benchmark-datasets',
    `${config.namePrefix}-datasets-`,
  );
  const resultsBucket = createProtectedBucket('benchmark-results', `${config.namePrefix}-results-`);

  return { datasetBucket, resultsBucket };
}
