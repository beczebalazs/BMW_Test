package BMW_Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Main {

	public static void main(String[] args) {
		Logger logger = Logger.getLogger("MyLog");
		logger.setUseParentHandlers(false);
		FileHandler fileHandler;
		String urlName = "https://jsonplaceholder.typicode.com/users";

		try {
			URL url = new URL(urlName);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.connect();

			Connection databaseConnection = null;

			String urlDatabase = "jdbc:mysql://localhost:3306/bmw_test";
			String userName = "root";
			String password = "";

			Class.forName("com.mysql.cj.jdbc.Driver");
			databaseConnection = DriverManager.getConnection(urlDatabase, userName, password);
			
			Statement constraintsOn = databaseConnection.createStatement();
			constraintsOn.executeUpdate("set foreign_key_checks = 0");
			Statement del1 = databaseConnection.createStatement();
			del1.executeUpdate("truncate company");
			Statement del2 = databaseConnection.createStatement();
			del2.executeUpdate("truncate address");
			Statement del3 = databaseConnection.createStatement();
			del3.executeUpdate("truncate users");
			Statement constraintsOff = databaseConnection.createStatement();
			constraintsOff.executeUpdate("set foreign_key_checks = 1");

			System.out.println("Connected to database!");


			fileHandler = new FileHandler("logfile.txt", true);
			logger.addHandler(fileHandler);
			SimpleFormatter formatter = new SimpleFormatter();
			fileHandler.setFormatter(formatter);
			logger.info("Trying to connect to: " + urlName);
			
			int statusCompany = 0;
			int statusAddress = 0;
			int statusUsers = 0;

			PreparedStatement statementCompany = databaseConnection.prepareStatement("insert into company values (?,?,?,?)");
			PreparedStatement statementAddress = databaseConnection.prepareStatement("insert into address values (?,?,?,?,?,?,?)");
			PreparedStatement statementUsers = databaseConnection.prepareStatement("insert into users values (?,?,?,?,?,?,?,?)");

			int responseCode = connection.getResponseCode();
			if (responseCode != 200) {
				logger.severe("Cannot access url! Response code: " + responseCode);
				throw new RuntimeException("Response code: " + responseCode);
			} else {
				logger.info("Url accessed successfully!");
				Scanner scanner = new Scanner(url.openStream());
				String line = "";
				while (scanner.hasNext()) {
					line += scanner.nextLine();
				}
				scanner.close();

				JSONParser parser = new JSONParser();
				JSONArray usersArray = (JSONArray) parser.parse(line);

				int idCompany = 1;
				int idAddress = 1;
				int idUsers = 1;
				System.out.println("Saving the data into the database...");

				for (int i = 0; i < usersArray.size(); ++i) {
					JSONObject users = (JSONObject) usersArray.get(i);
					JSONObject company = (JSONObject) users.get("company");
					JSONObject address = (JSONObject) users.get("address");

					String name = (String) company.get("name");
					String catchPhrase = (String) company.get("catchPhrase");
					String bs = (String) company.get("bs");
					
					String street = (String) address.get("street");
					String suite = (String) address.get("suite");
					String city = (String) address.get("city");
					String zipcode= (String) address.get("zipcode");

					JSONObject geo = (JSONObject) address.get("geo");
					String geo_lat = (String) geo.get("lat");
					String geo_lng = (String) geo.get("lng");
					
					statementAddress.setInt(1,idAddress++);
					statementAddress.setString(2, street);
					statementAddress.setString(3, suite);
					statementAddress.setString(4, city);
					statementAddress.setString(5, zipcode);
					statementAddress.setString(6, geo_lat);
					statementAddress.setString(7, geo_lng);

					statementCompany.setInt(1, idCompany++);
					statementCompany.setString(2, name);
					statementCompany.setString(3, catchPhrase);
					statementCompany.setString(4, bs);
					
					String usersName = (String) users.get("name");
					String userUsername = (String) users.get("username");
					int usersAddress = idUsers;
					String usersEmail = (String) users.get("email");
					String usersPhone = (String) users.get("phone");
					String usersWebsite = (String) users.get("website");
					int usersCompany = idUsers;
					
					statementUsers.setInt(1,idUsers++);
					statementUsers.setString(2,usersName);
					statementUsers.setString(3,userUsername);
					statementUsers.setString(4, usersEmail);
					statementUsers.setInt(5,usersAddress);
					statementUsers.setString(6,usersPhone);
					statementUsers.setString(7,usersWebsite);
					statementUsers.setInt(8,usersCompany);

					statusAddress = statementAddress.executeUpdate();
					statusCompany = statementCompany.executeUpdate();
					statusUsers = statementUsers.executeUpdate();

					String regex = "^(.+)@(.+)$";
					Pattern pattern = Pattern.compile(regex);
					Matcher matcher = pattern.matcher(users.get("email").toString());
					if (!matcher.matches()) {
						System.out.println("Email address format invalid for: " + users.get("email"));
					}
				}
				System.out.println("Database filled successfully!");
			}
			statementCompany.close();
			statementAddress.close();
			databaseConnection.close();
			connection.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
