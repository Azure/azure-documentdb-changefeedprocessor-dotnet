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
        public static DocumentCollection CreateCollection(
            string id,
            string rid,
            PartitionKeyDefinition partitionKey = null,
            string selfLink = "selfLink")
        {
            // Use dummy SelfLink.
            var collection = JsonConvert.DeserializeObject<DocumentCollection>(
                $"{{ \"id\": \"{id}\", \"_rid\": \"{rid}\", \"_self\": \"{selfLink}\" }}");
            if (partitionKey != null)
            {
                collection.PartitionKey = partitionKey;
            }

            return collection;
        }
    }
}
