// ==UserScript==
// @name Recommendations plugin for rd.springer.com
// @namespace     http://scattered-thoughts.net
// @require       http://ajax.googleapis.com/ajax/libs/jquery/1.5.1/jquery.min.js
// @include       http*://rd.springer.com/article/*
// ==/UserScript==

$(
    function () {
        var api_key = null; // get your own :-P

        var expander = $('#abstract-references').clone(true, true);
        expander[0].id = 'abstract-recommendations';
        expander.find('h2')[0].textContent = 'Recommendations (loading)'; 
        expander.find('ol').empty();
        expander.prependTo($('.document-aside'));

        // copied from minimized springer source
        var toggleExpander = function (_) {
            if ($(".expander-content", expander).is(":visible")) {
                expander.removeClass("expander-open").find(".expander-content").slideUp();
            } else {
                expander.addClass("expander-open").find(".expander-content").slideDown();
                if ($("#pub-date-graph").length) {
                    var d = new SearchResultsGraph();
                    d.init();
                }
            }
        };
        // end of copy

        expander.find('.expander-title').click(toggleExpander);

        var getTitle = function(doi, cont) {
            url = 'http://springer.api.mashery.com/metadata/json?api_key=' + api_key + '&q=' + encodeURIComponent('doi:' + doi);
            GM_xmlhttpRequest({
                                  method: 'GET',
                                  url: url,
                                  onload: function(response) {
                                      var title = $.parseJSON(response.responseText).records[0].title;
                                      cont(title);
                                  }
                              });
        };

        var ol = expander.find('ol');
        var addRecommendation = function (doi, score) {
            var url = 'http://rd.springer.com/article/' + doi;
            var a = '<a href="' + url + '"></a>';
            var span = '<span> (score ' + score.toFixed(3) + ')</span>';
            var li = $('<li>' + a + span + '</li>');
            ol.append(li);
            getTitle(doi,
                     function (title) {
                         li.find('a')[0].textContent = title;
                     });
        };

        var doi = document.getElementById('abstract-about-doi').textContent;
        var url = 'http://ec2-107-20-105-237.compute-1.amazonaws.com/api/recommendations/' + doi;
        GM_xmlhttpRequest({
                              method: 'GET',
                              url: url,       
                              onload: function(response) {
                                  var recommendations = $.parseJSON(response.responseText).recommendations;
                                  recommendations.sort();
                                  recommendations.reverse();
                                  
                                  $.each(recommendations,
                                         function (_, row) {
                                             addRecommendation(row[1], row[0]);
                                         });

                                  expander.find('h2')[0].textContent = 'Recommendations (' + recommendations.length + ')';
                              }
                          });
    }
);


