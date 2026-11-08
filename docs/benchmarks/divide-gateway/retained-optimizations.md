<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Retained gateway optimizations (2026-11-08)

This branch retains immutable upstream membership snapshots with mutation-compatible candidate filtering,
and the zero-retry execution path without changing configured retries. Existing key/candidate reuse is inherited from its base.

The response-header adapter, native-header experiment, URI shortcuts and random-generator replacement are not included.
The snapshot optimization demonstrates allocation reduction; it is not claimed as a stable standalone QPS gain.
The zero-retry path applies only when retry is configured to zero, while the default remains three.

Evidence and limitations: https://github.com/Aias00/shenyu-performance-lab

Validation: 133 targeted tests passed (upstream cache, random load balancing, breaker candidates,
Divide breaker behavior, zero retries, retries, timeout budget and request-body replay).
Checkstyle and Apache RAT results are preserved in the companion evidence repository.
