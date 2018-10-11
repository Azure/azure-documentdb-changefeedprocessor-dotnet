//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils
{
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Text;

    static class MockHelpers
    {
        public static DocumentCollection CreateCollection(string id, PartitionKeyDefinition partitionKey = null)
        {
            // Use dummy SelfLink.
            var collection = JsonConvert.DeserializeObject<DocumentCollection>(
                "{ \"id\": \"someCollection\", \"_self\": \"dbs/sY0YAA==/colls/sY0YAN3CXwA=/\" }");
            if (partitionKey != null)
            {
                collection.PartitionKey = partitionKey;
            }

            return collection;
        }
    }
}
