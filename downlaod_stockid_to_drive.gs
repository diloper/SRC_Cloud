function getstockFile(fileURL) {
 var response = UrlFetchApp.fetch('https://isin.twse.com.tw/isin/C_public.jsp?strMode=2');
  var fileBlob = response.getBlob()
//  Logger.log(response.getContentText("Big5"));
  
  
  var id = DriveApp.getFoldersByName('Stock db').next().getId();
  var folder = DriveApp.getFolderById(id);
  //DocsList.createFolder('Folder1')
  var files = DriveApp.searchFiles(
    'title = "isin.html" and trashed=false');
  while (files.hasNext()) {
  var file = files.next();
//    In order to use DriveAPI, you need to add it through the Resources, 
//    Advanced Google Services menu. Set the Drive API to ON. AND make sure that the Drive API is turned on in your Google Developers Console. 
//    If it's not turned on in BOTH places, it won't be available.
    rtrnFromDLET = Drive.Files.remove(file.getId());
  Logger.log(file.getName());
}
//    Logger.log(files);
  var result = folder.createFile('isin.html',response.getContentText("Big5"));
//   Logger.log(result);

}
