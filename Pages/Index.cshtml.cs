using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using DbMigratorApp.Services;

namespace DbMigratorApp.Pages;

public class IndexModel : PageModel
{
    private readonly DatabaseMigrationService _migrationService;

    public IndexModel(DatabaseMigrationService migrationService)
    {
        _migrationService = migrationService;
    }

    [BindProperty]
    public string MySqlConn { get; set; } = "";

    [BindProperty]
    public string MsSqlConn { get; set; } = "";

    public void OnGet()
    {
    }

    public IActionResult OnPostStartMigration()
    {
        if (string.IsNullOrEmpty(MySqlConn) || string.IsNullOrEmpty(MsSqlConn))
        {
            return Page();
        }

        _ = Task.Run(() => _migrationService.MigrateAsync(MySqlConn, MsSqlConn, 50));

        return RedirectToPage();
    }

    public IActionResult OnGetProgress()
    {
        return new JsonResult(new
        {
            isRunning = _migrationService.IsRunning,
            isCompleted = _migrationService.IsCompleted,
            currentTable = _migrationService.CurrentTable,
            processedCount = _migrationService.ProcessedCount,
            totalCount = _migrationService.TotalCount,
            errorMessage = _migrationService.ErrorMessage
        });
    }

    public IActionResult OnPostContinueMigration()
    {
        _migrationService.SkipCurrentTableAndContinue();
        return RedirectToPage();
    }
}