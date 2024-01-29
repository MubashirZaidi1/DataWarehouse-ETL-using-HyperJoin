package JavaTest;

public class MasterDataItem {
	 private int productId;
     private String productName;
     private double productPrice;
     private int supplierId;
     private String supplierName;
     private int storeId;
     private String storeName;

     public MasterDataItem(int productId, String productName, double productPrice, int supplierId, String supplierName, int storeId, String storeName) {
         this.productId = productId;
         this.productName = productName;
         this.productPrice = productPrice;
         this.supplierId = supplierId;
         this.supplierName = supplierName;
         this.storeId = storeId;
         this.storeName = storeName;
     }

     public int getProductId() {
         return productId;
     }

     public void setProductId(int productId) {
         this.productId = productId;
     }

     public String getProductName() {
         return productName;
     }

     public void setProductName(String productName) {
         this.productName = productName;
     }

     public double getProductPrice() {
         return productPrice;
     }

     public void setProductPrice(double productPrice) {
         this.productPrice = productPrice;
     }

     public int getSupplierId() {
         return supplierId;
     }

     public void setSupplierId(int supplierId) {
         this.supplierId = supplierId;
     }

     public String getSupplierName() {
         return supplierName;
     }

     public void setSupplierName(String supplierName) {
         this.supplierName = supplierName;
     }

     public int getStoreId() {
         return storeId;
     }

     public void setStoreId(int storeId) {
         this.storeId = storeId;
     }

     public String getStoreName() {
         return storeName;
     }

     public void setStoreName(String storeName) {
         this.storeName = storeName;
     }
 }


