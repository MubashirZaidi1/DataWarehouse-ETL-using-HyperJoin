package JavaTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public class MasterDataStreamGeneratorThread extends Thread {
    private static final String MASTER_JDBC_URL = "jdbc:mysql://localhost:3306/master";
    private BlockingQueue<MasterDataItem> masterStreamBuffer;
	private String username;
	private String password;

    public MasterDataStreamGeneratorThread(BlockingQueue<MasterDataItem> masterStreamBuffer,String username , String password) {
        this.masterStreamBuffer = masterStreamBuffer;
        this.username = username;
        this.password = password;
    }

    @Override
    public void run() {
        // JDBC URL of Master Data MySQL server
        String masterJdbcUrl = MASTER_JDBC_URL;

        // Database credentials (replace with your actual credentials)
    

        try {
            // Register JDBC driver and open connection
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(masterJdbcUrl, username, password);
            Statement statement = connection.createStatement();

            // Simulate a continuous stream of Master Data from the database
            while (true) {
                // Execute a query to fetch Master Data
                String query = "SELECT * FROM master_data";
                ResultSet resultSet = statement.executeQuery(query);

                // Read Master Data and add to the masterStreamBuffer
                while (resultSet.next()) {
                    int productId = resultSet.getInt("productId");
                    String productName = resultSet.getString("productName");
                    String productPriceString = resultSet.getString("productPrice");
                   
                    // Convert productPrice to double after removing the dollar sign
                    double productPrice = Double.parseDouble(productPriceString.replace("$", ""));
                    int supplierId = resultSet.getInt("supplierId");
                    String supplierName = resultSet.getString("supplierName");
                    int storeId = resultSet.getInt("storeId");
                    String storeName = resultSet.getString("storeName");

                    // Create a MasterDataItem object
                    MasterDataItem newData = new MasterDataItem(productId, productName, productPrice, supplierId, supplierName, storeId, storeName);

                    // Add the Master Data into the stream buffer
                    masterStreamBuffer.put(newData);

                    // Sleep for a short interval to simulate a stream
                    Thread.sleep(ThreadLocalRandom.current().nextInt(500, 1500));
                }

                // Close the result set to avoid resource leaks
                resultSet.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
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

		public int getJoinAttribute() {
			// TODO Auto-generated method stub
			return productId;
		}
    }
}
