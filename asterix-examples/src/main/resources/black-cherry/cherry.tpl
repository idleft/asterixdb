<!DOCTYPE html>
<html style="width: 100%; height: 100%;">
  <head>
    <title>ASTERIX Demo</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <link href="http://ajax.googleapis.com/ajax/libs/jqueryui/1.9.2/themes/base/jquery-ui.css"
        rel="stylesheet" type="text/css"/>
    <link href="static/css/bootstrap.min.css" rel="stylesheet" media="screen">

    <link rel="shortcut icon" type="image/png" href="static/img/hyrax.png">

    <script src="http://maps.googleapis.com/maps/api/js?sensor=false&libraries=places"
        type="text/javascript"></script> 
    <script src="http://code.jquery.com/jquery.min.js" type="text/javascript"></script>
    <script src="https://ajax.googleapis.com/ajax/libs/jqueryui/1.9.2/jquery-ui.min.js"
        type="text/javascript"></script>
  
    <script src="static/js/bootstrap.min.js"></script>
    <script type="text/javascript" src="static/js/geostats.js" ></script>

    <script src="static/js/asterix-sdk-stable.js"></script>
    <script src="static/js/cherry.js" type="text/javascript"></script>
    <style type="text/css">
        .column-section {
            border-top: 2px solid #aaa;
            margin-top: 10px;
        }

        .section-title {
            background: #ddd;
            padding: 5px 0;
        }

        .section-title span {
            margin-left: 10px;
            font-weight: bold;
        }
        
        .control {
            margin: 0;
        }
        
        #map_canvas img {
            max-width: none;
        }
        
        #legend-holder {
            background: white;
            padding: 10px;
            margin: 10px;
            text-align: center;
        }
        
        #review-handles-dropdown {
            padding: 0.5em;
        }

    </style>
  </head>
  <body style="width: 100%; height: 100%;">
    
    <!-- Nav -->
    <div class="navbar">
      <div class="navbar-inner">
        <a class="brand" href="#" style="padding: 0.25em;"><img src="static/img/finalasterixlogo.png" height="30" width="72"></a>
        <ul class="nav">
          <li id="explore-active" class="active"><a id="explore-mode" href="#">Explore</a></li>
          <li id="review-active" ><a id="review-mode" href="#">Review</a></li>
        </ul>
      </div>
    </div>    

    <!-- Search Box -->
    <div class="container-fluid">
    
      <div class="row">

        <!-- Search Bar -->
        <div class="span4 well" id="explore-well">
          <div class="column-section" id="location">
            <div class="section-title"><span>Location</span></div>
            <!--<div class="section-controls">-->
              <input class="textbox" type="text" id="location-text-box">
              <div class="btn-group" data-toggle="buttons-radio">
                <button type="button" class="btn" id="location-button">Location</button>
                <button type="button" class="btn" id="selection-button">Selection</button>
              </div>
            <!-- </div>-->
          </div><!-- end #location-->

          <div class="column-section" id="date">
            <div class="section-title"><span>Date</span></div>
            <div class="section-controls">
                <span style="white-space:nowrap; display: block; overflow:hidden;">
                    <label for="start-date">Start Date</label><input type="text" class="textbox" id="start-date">
                </span>
              <input type="text" class="textbox" id="end-date">
              <label for="end-date">End Date</label>
            </div>
          </div><!-- end #date-->

          <div class="column-section" id="keyword">
            <div class="section-title"><span>Keyword</span></div>
            <div class="section-controls">
              <input type="text" class="textbox" id="keyword-textbox" value="verizon">
            </div>
          </div><!-- end #keyword-->

          <div class="column-section" id="grid">
            <div class="section-title"><span>Grid: Latitude &amp; Longitude</span></div>
            <div class="section-controls">

              <div class="grid-slider"  id="grid-lat-slider"></div>
              <div class="control-label">Latitude: <span id="gridlat">3.0</span></div>

              <div class="grid-slider" id="grid-lng-slider"></div>
              <div class="control-label">Longitude: <span id="gridlng">3.0</span></div>
            </div>
          </div><!-- end #grid-->

          <div class="column-section" style="border: none;" id="submit-query">
            <div class="section-controls">
              	<button id="submit-button">Submit</button>
                <button id="clear-button">Clear</button>
                <button id="show-query-button">Show Query</button><br />
                <input type="checkbox" value="Submit Asynchronously" name="async" id="asbox" />
                Submit asynchronously?
            </div>
          </div><!-- end #submit-query -->
          
        </div><!--/span-->
        
         <!-- Drilldown Modal -->
          <div id="drilldown_modal" class="modal hide fade" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
            <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal" aria-hidden="true">x</button>
                <h3 id="myModalLabel">Explore Tweets</h3>
            </div>
            <div class="modal-body" id="drilldown_modal_body"></div>
            <div class="modal-footer">
                <button class="btn" data-dismiss="modal" aria-hidden="true">Close</button>
            </div>
          </div>
        
        <div class="span4 well" id="review-well" style="display:none;">
            <div class="btn-group" style="margin-bottom: 10px;" id="group-tweetbooks">
                <a class="btn dropdown-toggle" data-toggle="dropdown" href="#">
                    Tweetbooks
                    <span class="caret"></span>
                </a>
                <ul class="dropdown-menu" id="review-tweetbook-dropdown">
                    <li><a href="#" class="holdmenu">
                        <input type="text" id="new-tweetbook-entry" placeholder="Name a new tweetbook">
                        <button type="button" class="btn" id="new-tweetbook-button">Add</button>
                    </a></li>
                    <li class="divider"></li>
                    <div id="review-tweetbook-titles">
                    </div>
                </ul>
            </div><br/>
            
            <div class="btn-group" id="group-background-query" style="margin-bottom: 10px;">
                <a class="btn dropdown-toggle" data-toggle="dropdown" href="#">
                    Background Queries
                    <span class="caret"></span>
                </a>
                <ul class="dropdown-menu" id="review-handles-dropdown">
                    <div id="async-handle-controls">
                    </div>
                </ul>
            </div>
        </div>
        
        <!-- Map -->
        <div class="span8 well" >
          <div id="map_canvas" style="width: 100%; height: 100%;"></div>
        </div><!--/span-->
        <div id="map_canvas_legend">
        </div>
      
      </div><!--/row-->
      <div class="row">
      
         <div id="dialog">
            <h4>You must submit a query first.</h4>
        </div></div>
    </div><!-- container -->
    <hr>
    
  </body>
</html>
