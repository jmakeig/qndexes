(
xdmp:set-response-content-type("text/html;charset=utf-8"),
"<!DOCTYPE html>",
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
	<meta charset="utf-8" />
	<title>Qndexes</title>
	<link type="text/css" rel="stylesheet" href="/static/browser.css" />
	<script type="text/javascript" src="static/jquery.js" />
	<script type="text/javascript" src="static/app.js?{xdmp:random()}" />
</head>
<body>
  <h1>Qndexes</h1>
  <h2>“Because there’s no ‘U’ in QName”</h2>
  
  <form method="get" id="search">
    <input name="q" id="q" placeholder="Search"/>
    <button id="submit">Search</button>
  </form>
  <div class="panel">
    <h3>Element-Element</h3>
    <div id="element_element" class="container">element_element</div>
  </div>
  <div class="panel">
    <h3>Element</h3>
    <div id="element" class="container">element</div>
  </div>
</body>
</html>
)
