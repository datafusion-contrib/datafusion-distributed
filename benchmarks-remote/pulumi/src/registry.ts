import * as aws from '@pulumi/aws';

import { engineNames, EngineName, FoundationConfig } from './config';

export type EngineRepositories = Record<EngineName, aws.ecr.Repository>;

export function createRepositories(config: FoundationConfig): EngineRepositories {
  return Object.fromEntries(
    engineNames.map((engine) => {
      const repository = new aws.ecr.Repository(`benchmark-${engine}-repository`, {
        name: `${config.namePrefix}/${engine}`,
        imageTagMutability: 'IMMUTABLE',
        imageScanningConfiguration: {
          scanOnPush: true,
        },
        encryptionConfigurations: [
          {
            encryptionType: 'AES256',
          },
        ],
        forceDelete: true,
        tags: {
          'benchmark.datafusion.apache.org/engine': engine,
        },
      });

      new aws.ecr.LifecyclePolicy(`benchmark-${engine}-lifecycle`, {
        repository: repository.name,
        policy: JSON.stringify({
          rules: [
            {
              rulePriority: 1,
              description: 'Remove untagged development images after seven days',
              selection: {
                tagStatus: 'untagged',
                countType: 'sinceImagePushed',
                countUnit: 'days',
                countNumber: 7,
              },
              action: { type: 'expire' },
            },
          ],
        }),
      });

      return [engine, repository];
    }),
  ) as EngineRepositories;
}
