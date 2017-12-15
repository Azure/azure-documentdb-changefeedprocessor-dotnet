using System;
using DocumentDB.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.Client;

namespace DocumentDB.ChangeFeedProcessor
{
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
            ConnectionPolicy = new ConnectionPolicy();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentCollectionInfo"/> class.
        /// </summary>
        /// <param name="other">The other <see cref="DocumentCollectionInfo"/> to copy settings from.</param>
        public DocumentCollectionInfo(DocumentCollectionInfo other)
        {
            Uri = other.Uri;
            MasterKey = other.MasterKey;
            DatabaseName = other.DatabaseName;
            CollectionName = other.CollectionName;
            ConnectionPolicy = other.ConnectionPolicy;
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
        /// Gets or sets the connection policy to connect to Document service.
        /// </summary>
        public ConnectionPolicy ConnectionPolicy { get; set; }
    }
}