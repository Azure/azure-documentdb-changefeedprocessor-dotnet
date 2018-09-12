//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Holds information specifying how to get Document collection.
    /// </summary>
    public class DocumentCollectionInfo
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentCollectionInfo"/> class.
        /// </summary>
        public DocumentCollectionInfo()
        {
            this.ConnectionPolicy = new ConnectionPolicy();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentCollectionInfo"/> class.
        /// </summary>
        /// <param name="other">The other <see cref="DocumentCollectionInfo"/> to copy settings from.</param>
        public DocumentCollectionInfo(DocumentCollectionInfo other)
        {
            this.Uri = other.Uri;
            this.MasterKey = other.MasterKey;
            this.DatabaseName = other.DatabaseName;
            this.CollectionName = other.CollectionName;
            this.ConnectionPolicy = other.ConnectionPolicy;
        }

        /// <summary>
        /// Gets or sets the Uri of the Document service.
        /// </summary>
        public Uri Uri { get; set; }

        /// <summary>
        /// Gets or sets the secret master key to connect to the Document service.
        /// </summary>
        public string MasterKey { get; set; }

        /// <summary>
        /// Gets or sets the name of the database the collection resides in.
        /// </summary>
        public string DatabaseName { get; set; }

        /// <summary>
        /// Gets or sets the name of the Document collection.
        /// </summary>
        public string CollectionName { get; set; }

        /// <summary>
        /// Gets or sets the consistency level. Default is null which means it will be used a collection level consistency.
        /// </summary>
        public ConsistencyLevel? ConsistencyLevel { get; set; }

        /// <summary>
        /// Gets or sets the connection policy to connect to Document service.
        /// </summary>
        public ConnectionPolicy ConnectionPolicy { get; set; }
    }
}