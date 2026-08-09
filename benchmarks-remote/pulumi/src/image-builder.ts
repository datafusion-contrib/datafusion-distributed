import * as aws from '@pulumi/aws';

import { FoundationConfig } from './config';
import { BenchmarkStorage } from './storage';

const codeBuildAssumeRolePolicy = JSON.stringify({
  Version: '2012-10-17',
  Statement: [
    {
      Effect: 'Allow',
      Principal: { Service: 'codebuild.amazonaws.com' },
      Action: 'sts:AssumeRole',
    },
  ],
});

export function createImageBuilder(
  config: FoundationConfig,
  storage: BenchmarkStorage,
  sparkRepository: aws.ecr.Repository,
): aws.codebuild.Project {
  const role = new aws.iam.Role('benchmark-image-builder-role', {
    assumeRolePolicy: codeBuildAssumeRolePolicy,
    tags: { Name: `${config.namePrefix}-image-builder` },
  });
  new aws.iam.RolePolicy('benchmark-image-builder-policy', {
    role: role.id,
    policy: storage.resultsBucket.arn.apply((resultsArn) =>
      sparkRepository.arn.apply((repositoryArn) =>
        JSON.stringify({
          Version: '2012-10-17',
          Statement: [
            {
              Sid: 'ReadBuildContext',
              Effect: 'Allow',
              Action: ['s3:GetObject'],
              Resource: `${resultsArn}/runs/bootstrap/images/*`,
            },
            {
              Sid: 'AuthenticateToEcr',
              Effect: 'Allow',
              Action: ['ecr:GetAuthorizationToken'],
              Resource: '*',
            },
            {
              Sid: 'PublishSparkImage',
              Effect: 'Allow',
              Action: [
                'ecr:BatchCheckLayerAvailability',
                'ecr:CompleteLayerUpload',
                'ecr:InitiateLayerUpload',
                'ecr:PutImage',
                'ecr:UploadLayerPart',
              ],
              Resource: repositoryArn,
            },
            {
              Sid: 'WriteBuildLogs',
              Effect: 'Allow',
              Action: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
              Resource: '*',
            },
          ],
        }),
      ),
    ),
  });

  return new aws.codebuild.Project('benchmark-image-builder', {
    name: `${config.namePrefix}-image-builder`,
    serviceRole: role.arn,
    buildTimeout: 60,
    artifacts: { type: 'NO_ARTIFACTS' },
    environment: {
      computeType: 'BUILD_GENERAL1_SMALL',
      image: 'aws/codebuild/standard:7.0',
      type: 'LINUX_CONTAINER',
      privilegedMode: true,
      imagePullCredentialsType: 'CODEBUILD',
    },
    source: {
      type: 'NO_SOURCE',
      buildspec: `version: 0.2
phases:
  pre_build:
    commands:
      - aws s3 cp "$BUILD_CONTEXT" /tmp/context.tar.gz
      - mkdir -p /tmp/context
      - tar -xzf /tmp/context.tar.gz -C /tmp/context
      - registry="\${IMAGE_URI%%/*}"
      - aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$registry"
  build:
    commands:
      - docker build --pull -t "$IMAGE_URI" /tmp/context
  post_build:
    commands:
      - docker push "$IMAGE_URI"
`,
    },
    tags: { Name: `${config.namePrefix}-image-builder` },
  });
}
