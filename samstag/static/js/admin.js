function editTeam(teamId, teamName, groupNumber) {
    $('#editTeamId').val(teamId);
    $('#editTeamName').val(teamName);
    $('#editGroupNumber').val(groupNumber);
    $('#editTeamModal').modal('show');
}

function deleteTeam(teamId, teamName) {
    if (confirm('Möchtest du das Team "' + teamName + '" wirklich löschen? Alle zugehörigen Ergebnisse werden gelöscht!')) {
        var gameName = $('#gameName').val() || window.location.pathname.split('/')[2];

        $.post('/delete_team/' + gameName + '/' + teamId, function (data) {
            if (data.success) {
                location.reload();
            } else {
                alert('Fehler: ' + data.error);
            }
        });
    }
}

$(document).ready(function () {
    // Add Team
    $('#addTeamForm').submit(function (e) {
        e.preventDefault();
        var gameName = $('#gameName').val();

        $.post('/add_team/' + gameName, {
            team_name: $('#teamName').val(),
            group_number: $('#groupNumber').val()
        }, function (data) {
            if (data.success) {
                location.reload();
            } else {
                alert('Fehler: ' + data.error);
            }
        });
    });

    // Edit Team
    $('#editTeamForm').submit(function (e) {
        e.preventDefault();
        var gameName = $('#gameName').val() || window.location.pathname.split('/')[2];
        var teamId = $('#editTeamId').val();

        $.post('/edit_team/' + gameName + '/' + teamId, {
            new_name: $('#editTeamName').val(),
            new_group: $('#editGroupNumber').val()
        }, function (data) {
            if (data.success) {
                location.reload();
            } else {
                alert('Fehler: ' + data.error);
            }
        });
    });
});

function generateGhostTeams(gameName) {
    $.post('/generate_ghost_teams/' + gameName, function (data) {
        if (data.success) {
            alert(data.generated + ' Ghost-Teams erfolgreich generiert!');
            location.reload();
        } else {
            alert('Fehler: ' + data.error);
        }
    });
}
