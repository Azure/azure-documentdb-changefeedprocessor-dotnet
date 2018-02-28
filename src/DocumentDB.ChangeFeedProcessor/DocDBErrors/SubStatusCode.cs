//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DocDBErrors
{
    internal enum SubStatusCode
    {
        Undefined = -1,

        /// <summary>
        /// 410: partition key range is gone
        /// </summary>
        PartitionKeyRangeGone = 1002,

        /// <summary>
        /// 410: partition splitting
        /// </summary>
        Splitting = 1007,

        /// <summary>
        /// 404: LSN in session token is higher
        /// </summary>
        ReadSessionNotAvailable = 1002,
    }
}