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

test('requires a pinned k3s release for the self-managed backend', () => {
  const config = testConfig();
  config.k3sVersion = 'latest';

  assert.throws(() => validateFoundationConfig(config), /k3sVersion must be an exact release/);
});

test('rejects a k3s pod CIDR that overlaps the VPC', () => {
  const config = testConfig();
  config.k3sPodCidr = '10.42.128.0/17';

  assert.throws(() => validateFoundationConfig(config), /k3sPodCidr must not overlap vpcCidr/);
});
