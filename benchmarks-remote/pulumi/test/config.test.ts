import assert from 'node:assert/strict';
import test from 'node:test';

import { validateFoundationConfig } from '../src/config';
import { testConfig } from './fixture';

test('accepts a pinned and isolated benchmark configuration', () => {
  const config = testConfig();
  assert.equal(validateFoundationConfig(config), config);
});

test('requires two distinct availability zones', () => {
  const config = testConfig();
  config.availabilityZones = ['eu-west-1a', 'eu-west-1a'];
  assert.throws(() => validateFoundationConfig(config), /must be distinct/);
});

test('requires a positive benchmark node limit', () => {
  const config = testConfig();
  config.benchmarkNodeCount = 0;
  assert.throws(() => validateFoundationConfig(config), /positive integer/);
});

test('requires a pinned EKS minor release', () => {
  const config = testConfig();
  config.eksVersion = 'latest';

  assert.throws(() => validateFoundationConfig(config), /eksVersion must be an exact EKS minor/);
});

test('rejects a world-accessible Kubernetes API', () => {
  const config = testConfig();
  config.kubernetesApiAllowedCidrs = ['0.0.0.0/0'];

  assert.throws(() => validateFoundationConfig(config), /must not expose/);
});
