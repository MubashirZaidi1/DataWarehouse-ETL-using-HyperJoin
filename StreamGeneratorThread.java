package JavaTest;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public class StreamGeneratorThread extends Thread {
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/transaction";
    private static final int STREAM_BUFFER_CAPACITY = 100;
    private static final int DISK_BUFFER_CAPACITY = 10;

    private BlockingQueue<DataItem> streamBuffer;
    private MultiValuedMap<Integer, DataItem> multiHashTable;
    private BlockingQueue<DataItem> diskBuffer;
    private BlockingQueue<MasterDataStreamGeneratorThread.MasterDataItem> masterStreamBuffer; // Add this field
	private String username;
	private String password;

    public StreamGeneratorThread(BlockingQueue<DataItem> streamBuffer, MultiValuedMap<Integer, DataItem> multiHashTable,
                                 BlockingQueue<DataItem> diskBuffer, BlockingQueue<MasterDataStreamGeneratorThread.MasterDataItem> masterStreamBuffer, String username, String password) {
        this.streamBuffer = streamBuffer;
        this.multiHashTable = multiHashTable;
        this.diskBuffer = diskBuffer;
        this.masterStreamBuffer = masterStreamBuffer; // Initialize the masterStreamBuffer
        this.username = username;
        this.password = password;
    }

    // Other methods remain unchanged


    
    @Override
    public void run() {
        String jdbcUrl = JDBC_URL;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            Statement statement = connection.createStatement();

            while (true) {
                String query = "SELECT * FROM transactions";
                ResultSet resultSet = statement.executeQuery(query);

                int itemsRead = 0;
                while (itemsRead < STREAM_BUFFER_CAPACITY && resultSet.next()) {
                    int orderId = resultSet.getInt("Order ID");
                    String orderDate = resultSet.getString("Order Date");
                    int productId = resultSet.getInt("ProductID");
                    int customerId = resultSet.getInt("CustomerID");
                    String customerName = resultSet.getString("CustomerName");
                    String gender = resultSet.getString("Gender");
                    int quantityOrdered = resultSet.getInt("Quantity Ordered");

                    DataItem newData = new DataItem(orderId, orderDate, productId, customerId, customerName, gender, quantityOrdered);

                    streamBuffer.put(newData);
                    int joinAttribute = newData.getJoinAttribute();

                    synchronized (multiHashTable) {
                        multiHashTable.put(joinAttribute, newData);
                    }

                    if (diskBuffer.size() < DISK_BUFFER_CAPACITY) {
                        diskBuffer.put(newData);
                    } else {
                        DataItem oldestNode = diskBuffer.poll();
                        diskBuffer.put(newData);
                    }

                    itemsRead++;
                    Thread.sleep(ThreadLocalRandom.current().nextInt(500, 1500));
                }
              

                // Display buffer contents (for demonstration)
                System.out.println("Stream Buffer Contents:");
                streamBuffer.forEach(System.out::println);
                System.out.println("Multi-Hash Table Contents:");
                multiHashTable.entries().forEach(entry -> System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue()));
                System.out.println("Disk Buffer Contents:");
                diskBuffer.forEach(System.out::println);

                resultSet.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	static List<Integer> productIDsToUpdate = new ArrayList<>();

    public void matchRecordsAndProduceOutputAndLoadToDW() {
        try {
            int outputCount = 0;
            while (outputCount < 50) {
                DataItem recordFromDisk = diskBuffer.take();
                int transactionProductId = recordFromDisk.getProductId();

                synchronized (multiHashTable) {
                    int joinAttribute = recordFromDisk.getJoinAttribute();

                    // Check if there is a matching MasterDataItem based on the join attribute
                    MasterDataStreamGeneratorThread.MasterDataItem masterItem = getMasterItem(joinAttribute);
                    if (masterItem != null && masterItem.getProductId() == transactionProductId) {
                        // Matching record found, calculate TOTAL_SALE and update DataItem
                        double totalPrice = masterItem.getProductPrice() * recordFromDisk.getQuantityOrdered();
                       
                        // Produce the output tuple
                        System.out.println("Matching Product ID: " + transactionProductId);
                        System.out.println("Transaction Data: " + recordFromDisk);
                        System.out.println("Master Data: " + masterItem);

                        // Update the multi-hash table and remove the matched tuple
                        multiHashTable.remove(joinAttribute);

                        // Load tuple into DW (pseudo code - replace with your DW logic)
                        // Check if dimensions tables contain the information
                        // If not, update dimensions and fact tables in the DW
                        // Enter foreign keys in the fact table
                        productIDsToUpdate.add(transactionProductId);
                        // Update electronica database's order_table
                        updateElectronicaOrderTable(recordFromDisk);
                        updateElectronicaCustomerTable(recordFromDisk);
                        updateElectronicaStoreTable(masterItem);
                        updateElectronicaSupplierTable(masterItem);
                        updateElectronicaProductTable(masterItem);
                        

                        outputCount++;
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private void updateElectronicaOrderTable(DataItem recordFromDisk) {
        String electronicaJdbcUrl = "jdbc:mysql://localhost:3306/electronica";
        String electronicaUsername = "root";
        String electronicaPassword = "root";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection electronicaConnection = DriverManager.getConnection(electronicaJdbcUrl, electronicaUsername, electronicaPassword);
            electronicaConnection.setAutoCommit(true);

            int orderId = recordFromDisk.getOrderId();
            String orderDate = recordFromDisk.getOrderDate();

            String updateQuery = "UPDATE order_table SET orderID = ?, orderDate = ?";
            PreparedStatement electronicaStatement = electronicaConnection.prepareStatement(updateQuery);
            electronicaStatement.setInt(1, orderId);
            electronicaStatement.setString(2, orderDate);

            int rowsAffected = electronicaStatement.executeUpdate();
            System.out.println("Executing query: " + electronicaStatement.toString()); // Print the prepared statement

            if (rowsAffected > 0) {
                System.out.println("Order_table in 'electronica' database updated");
            } else {
                System.out.println("No rows were updated");
            }

            electronicaStatement.close();
            electronicaConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("SQL Exception occurred: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Class not found exception occurred: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred: " + e.getMessage());
        }
    }
    private void updateElectronicaCustomerTable(DataItem recordFromDisk) {
        String electronicaJdbcUrl = "jdbc:mysql://localhost:3306/electronica";
        String electronicaUsername = "root";
        String electronicaPassword = "root";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection electronicaConnection = DriverManager.getConnection(electronicaJdbcUrl, electronicaUsername, electronicaPassword);
            electronicaConnection.setAutoCommit(true);

            int customerId = recordFromDisk.getCustomerId();
            String customerName = recordFromDisk.getCustomerName();
            String gender = recordFromDisk.getGender();

            String updateQuery = "UPDATE customer_table SET CustomerName = ?, Gender = ? WHERE CustomerID = ?";
            PreparedStatement electronicaStatement = electronicaConnection.prepareStatement(updateQuery);
            electronicaStatement.setString(1, customerName);
            electronicaStatement.setString(2, gender);
            electronicaStatement.setInt(3, customerId);

            int rowsAffected = electronicaStatement.executeUpdate();
            System.out.println("Executing query: " + electronicaStatement.toString()); // Print the prepared statement

            if (rowsAffected > 0) {
                System.out.println("customer_table in 'electronica' database updated");
            } else {
                System.out.println("No rows were updated");
            }

            electronicaStatement.close();
            electronicaConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("SQL Exception occurred: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Class not found exception occurred: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred: " + e.getMessage());
        }
    }
    private void updateElectronicaStoreTable(MasterDataStreamGeneratorThread.MasterDataItem masterItem) {
        String electronicaJdbcUrl = "jdbc:mysql://localhost:3306/electronica";
        String electronicaUsername = "root";
        String electronicaPassword = "root";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection electronicaConnection = DriverManager.getConnection(electronicaJdbcUrl, electronicaUsername, electronicaPassword);
            electronicaConnection.setAutoCommit(true);

            int storeId = masterItem.getStoreId();
            String storeName = masterItem.getStoreName();

            String updateQuery = "UPDATE store_table SET StoreName = ? WHERE StoreID = ?";
            PreparedStatement electronicaStatement = electronicaConnection.prepareStatement(updateQuery);
            electronicaStatement.setString(1, storeName);
            electronicaStatement.setInt(2, storeId);

            int rowsAffected = electronicaStatement.executeUpdate();
            System.out.println("Executing query: " + electronicaStatement.toString()); // Print the prepared statement

            if (rowsAffected > 0) {
                System.out.println("store_table in 'electronica' database updated");
            } else {
                System.out.println("No rows were updated");
            }

            electronicaStatement.close();
            electronicaConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("SQL Exception occurred: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Class not found exception occurred: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred: " + e.getMessage());
        }
    }
    private void updateElectronicaSupplierTable(MasterDataStreamGeneratorThread.MasterDataItem masterItem) {
        String electronicaJdbcUrl = "jdbc:mysql://localhost:3306/electronica";
        String electronicaUsername = "root";
        String electronicaPassword = "root";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection electronicaConnection = DriverManager.getConnection(electronicaJdbcUrl, electronicaUsername, electronicaPassword);
            electronicaConnection.setAutoCommit(true);

            int supplierId = masterItem.getSupplierId();
            String supplierName = masterItem.getSupplierName();

            String updateQuery = "UPDATE supplier_table SET SupplierName = ? WHERE SupplierID = ?";
            PreparedStatement electronicaStatement = electronicaConnection.prepareStatement(updateQuery);
            electronicaStatement.setString(1, supplierName);
            electronicaStatement.setInt(2, supplierId);

            int rowsAffected = electronicaStatement.executeUpdate();
            System.out.println("Executing query: " + electronicaStatement.toString()); // Print the prepared statement

            if (rowsAffected > 0) {
                System.out.println("supplier_table in 'electronica' database updated");
            } else {
                System.out.println("No rows were updated");
            }

            electronicaStatement.close();
            electronicaConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("SQL Exception occurred: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Class not found exception occurred: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred: " + e.getMessage());
        }
    }
    private void updateElectronicaProductTable(MasterDataStreamGeneratorThread.MasterDataItem masterItem) {
        String electronicaJdbcUrl = "jdbc:mysql://localhost:3306/electronica";
        String electronicaUsername = "root";
        String electronicaPassword = "root";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection electronicaConnection = DriverManager.getConnection(electronicaJdbcUrl, electronicaUsername, electronicaPassword);
            electronicaConnection.setAutoCommit(true);

            int productId = masterItem.getProductId();
            String productName = masterItem.getProductName();
            double productPrice = masterItem.getProductPrice();

            String updateQuery = "UPDATE product_table SET ProductName = ?, ProductPrice = ? WHERE ProductID = ?";
            PreparedStatement electronicaStatement = electronicaConnection.prepareStatement(updateQuery);
            electronicaStatement.setString(1, productName);
            electronicaStatement.setDouble(2, productPrice);
            electronicaStatement.setInt(3, productId);

            int rowsAffected = electronicaStatement.executeUpdate();
            System.out.println("Executing query: " + electronicaStatement.toString()); // Print the prepared statement

            if (rowsAffected > 0) {
                System.out.println("product_table in 'electronica' database updated");
            } else {
                System.out.println("No rows were updated");
            }

            electronicaStatement.close();
            electronicaConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("SQL Exception occurred: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Class not found exception occurred: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred: " + e.getMessage());
        }
    }




    private MasterDataStreamGeneratorThread.MasterDataItem getMasterItem(int joinAttribute) {
        for (MasterDataStreamGeneratorThread.MasterDataItem masterItem : masterStreamBuffer) {
            if (masterItem.getJoinAttribute() == joinAttribute) {
                return masterItem;
            }
        }
        return null;
    }




  
    
    public static void main(String[] args) {
    	Scanner scanner = new Scanner(System.in);

        System.out.print("Enter database username: ");
        String username = scanner.nextLine();

        System.out.print("Enter database password: ");
        String password = scanner.nextLine();
    	BlockingQueue<DataItem> streamBuffer = new ArrayBlockingQueue<>(STREAM_BUFFER_CAPACITY);
        MultiValuedMap<Integer, DataItem> multiHashTable = new ArrayListValuedHashMap<>();
        BlockingQueue<DataItem> diskBuffer = new ArrayBlockingQueue<>(DISK_BUFFER_CAPACITY);
        BlockingQueue<MasterDataStreamGeneratorThread.MasterDataItem> masterStreamBuffer = new ArrayBlockingQueue<>(STREAM_BUFFER_CAPACITY);
        StreamGeneratorThread streamGeneratorThread = new StreamGeneratorThread(streamBuffer, multiHashTable, diskBuffer, masterStreamBuffer,username,password);

        MasterDataStreamGeneratorThread masterDataStreamGeneratorThread = new MasterDataStreamGeneratorThread(masterStreamBuffer,username,password);

        streamGeneratorThread.start();
        masterDataStreamGeneratorThread.start();

        try {
            Thread.sleep(2000); // Wait for a brief moment to allow data to be fetched into the stream buffers
            // Logic to process the transaction data and match with master data
            streamGeneratorThread.matchRecordsAndProduceOutputAndLoadToDW();
            System.out.println("Product IDs to Update: " + productIDsToUpdate);
            


  
    }
     catch (InterruptedException e) {
        e.printStackTrace();
    }

}



public class DataItem {
    // Assume this is your data structure
    // Add other attributes as needed
    private int orderId;
    private String orderDate;
    private int productId;
    private int customerId;
    private String customerName;
    private String gender;
    private int quantityOrdered;

    public DataItem(int orderId, String orderDate, int productId, int customerId, String customerName, String gender, int quantityOrdered) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.productId = productId;
        this.customerId = customerId;
        this.customerName = customerName;
        this.gender = gender;
        this.quantityOrdered = quantityOrdered;
    }

   

	public int getJoinAttribute() {
		// TODO Auto-generated method stub
		return productId;
	}

	public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getQuantityOrdered() {
        return quantityOrdered;
    }

    public void setQuantityOrdered(int quantityOrdered) {
        this.quantityOrdered = quantityOrdered;
    }





    @Override
    public String toString() {
        return "DataItem{" +
                "orderId=" + orderId +
                ", orderDate='" + orderDate + '\'' +
                ", productId=" + productId +
                ", customerId=" + customerId +
                ", customerName='" + customerName + '\'' +
                ", gender='" + gender + '\'' +
                ", quantityOrdered=" + quantityOrdered +
                '}';
    }
}}


