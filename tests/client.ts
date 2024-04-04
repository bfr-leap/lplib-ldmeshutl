import { kSvcRequest } from '../src/ldata-mesh-util/ksvc-client';
import type { KSvcRequest } from '../src/ldata-mesh-util/ksvc-client';

async function main() {
    console.log('Hello from client test file');

    let req: KSvcRequest = {
        source: 'test-source',
        target: 'test-service',
        key: 'test-key',
        timestamp: Date.now(),
        request: {}
    };

    let res = await kSvcRequest(req);
    console.log('ksvcRequest:', res);
}

main();

