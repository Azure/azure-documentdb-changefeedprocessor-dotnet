//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;

    /// <summary>
    /// Internal metadata/attributes of the collection object.
    /// </summary>
    internal class CollectionMetadata
    {
        public CollectionMetadata(string selfLink, bool isPatititioned)
        {
            if (string.IsNullOrEmpty(selfLink)) throw new ArgumentException(nameof(selfLink) + " cannot be null or empty.", nameof(selfLink));

            this.SelfLink = selfLink;
            this.IsPartitioned = isPatititioned;
        }

        public string SelfLink { get; private set; }

        public bool IsPartitioned { get; private set; }
    }
}
